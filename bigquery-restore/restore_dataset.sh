#!/usr/bin/env bash
# BigQuery Dataset/Table Restore with Logging & Auto-Discovery (Bash 3.2 compatible)

set -Eeuo pipefail

# ------------------------- USER CONFIG --------------------------------
PROJECT_ID="${PROJECT_ID:-yoir-project-name}"
SOURCE_DATASET_ID="${SOURCE_DATASET_ID:-your-deleted-dataset-name}"   # historical (same as target)
TARGET_DATASET_ID="${TARGET_DATASET_ID:-your-target-dataset-name}"   # current dataset to restore into
LOCATION="${LOCATION:-your-location}"

# Choose one (epoch ms preferred for bq cp). Keep both if you want cp + CTAS fallback.
SNAPSHOT_EPOCH_MS="${SNAPSHOT_EPOCH_MS:-1756107767930}"            # 2025-08-25T10:42:47.930687Z
SNAPSHOT_TIMESTAMP="${SNAPSHOT_TIMESTAMP:-2025-08-25 10:42:37 UTC}" # a few seconds BEFORE delete

# If you already know table names, add them here. Otherwise, auto-discover from logs.
KNOWN_TABLES=(
  "example_table_1"
  "example_table_2"
)

# Allow a comma-separated override from env: TABLES_CSV="t1,t2,..."
if [ -n "${TABLES_CSV:-}" ]; then
  IFS=',' read -r -a KNOWN_TABLES <<< "$TABLES_CSV"
fi

# ------------------------- LOGGING (Bash 3.2 safe) --------------------
timestamp() { date -u +'%Y-%m-%dT%H:%M:%SZ'; }
log()  { echo "[$(timestamp)] [$1] $2"; }
info() { log INFO "$1"; }
warn() { log WARN "$1"; }
err()  { log ERROR "$1"; }

# ------------------------- LOG FILE -----------------------------------
TIMESTAMP="$(date -u +'%Y-%m-%dT%H-%M-%SZ')"
LOG_DIR="${LOG_DIR:-./logs}"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/restore_${SOURCE_DATASET_ID}_${TIMESTAMP}.log"
exec > >(tee -a "$LOG_FILE") 2>&1
[ "${DEBUG:-}" = "1" ] && set -x

RESTORED_OK=()
RESTORED_FAIL=()

cleanup() {
  code=$?
  if [ $code -ne 0 ]; then err "Script exited with code $code"; fi
  info "Log saved to: $LOG_FILE"
}
trap cleanup EXIT

# ------------------------- PRECHECKS ----------------------------------
need() { command -v "$1" >/dev/null 2>&1 || { err "Missing command: $1"; exit 2; }; }
need bq
if ! command -v gcloud >/dev/null 2>&1; then warn "gcloud not found; table auto-discovery from logs will be skipped"; fi

# Validate target dataset existence & get its actual location
DATASET_LOC="$(bq --project_id="$PROJECT_ID" show -d --format=prettyjson "$PROJECT_ID:$TARGET_DATASET_ID" \
  | (command -v jq >/dev/null 2>&1 && jq -r .location || sed -n 's/.*"location": "\(.*\)".*/\1/p'))" || DATASET_LOC=""

if [ -z "$DATASET_LOC" ]; then
  err "Target dataset $PROJECT_ID:$TARGET_DATASET_ID not found. Create it in $LOCATION first."
  exit 3
fi
if [ "$DATASET_LOC" != "$LOCATION" ]; then
  warn "Target dataset is in '$DATASET_LOC' but LOCATION is '$LOCATION'. Using dataset's actual location."
  LOCATION="$DATASET_LOC"
fi

info "Project:                     $PROJECT_ID"
info "Source dataset (historical): $SOURCE_DATASET_ID"
info "Target dataset (current):    $TARGET_DATASET_ID"
info "Location:                    $LOCATION"
info "Snapshot epoch ms:           ${SNAPSHOT_EPOCH_MS:-<none>}"
info "Snapshot timestamp:          ${SNAPSHOT_TIMESTAMP:-<none>}"
info "Log file:                    $LOG_FILE"
echo

# ------------------------- DISCOVER TABLES ----------------------------
TABLES=()
if [ ${#KNOWN_TABLES[@]} -gt 0 ]; then
  info "Using KNOWN_TABLES provided (${#KNOWN_TABLES[@]})."
  TABLES=( "${KNOWN_TABLES[@]}" )

elif command -v gcloud >/dev/null 2>&1; then
  info "Discovering table names from Cloud Audit Logs (±1 יום סביב ה-SNAPSHOT_TIMESTAMP)…"
  TMP_LIST="/tmp/${SOURCE_DATASET_ID}_tables_${TIMESTAMP}.txt"

  # נחשב חלון זמן אוטומטי סביב SNAPSHOT_TIMESTAMP:
  # macOS/BSD date ו-GNU date שונים — נשתמש ב-python אם קיים, אחרת ניפול לחלון קבוע.
  START_DATE=""
  END_DATE=""

  if command -v python3 >/dev/null 2>&1; then
    read -r START_DATE END_DATE <<PYOUT
$(python3 - <<'PY'
import os, datetime
ts = os.environ.get("SNAPSHOT_TIMESTAMP", "2025-08-25 10:42:37 UTC").replace(" UTC","")
dt = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
start = (dt - datetime.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
end   = (dt + datetime.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
print(start, end)
PY
)
PYOUT
  fi

  # אם אין python3 — נ fallback לחלון ידני של תאריכים ידועים (לשנות לפי הצורך)
  [ -z "$START_DATE" ] && START_DATE="2025-08-24T00:00:00Z"
  [ -z "$END_DATE" ] && END_DATE="2025-08-26T23:59:59Z"

  # חיפוש בשדות resource.labels.table_id
  gcloud logging read \
    "resource.labels.dataset_id=$SOURCE_DATASET_ID
     AND timestamp>=\"$START_DATE\" AND timestamp<=\"$END_DATE\"" \
     --project="$PROJECT_ID" --limit=5000 --format="value(resource.labels.table_id)" \
  | sort -u > "$TMP_LIST" || true

  # חיפוש נוסף לפי protoPayload.resourceName (למקרים שנרשם ברמת dataset)
  gcloud logging read \
    "protoPayload.resourceName:\"projects/$PROJECT_ID/datasets/$SOURCE_DATASET_ID\"
     AND timestamp>=\"$START_DATE\" AND timestamp<=\"$END_DATE\"" \
     --project="$PROJECT_ID" --limit=5000 --format=json \
  | (command -v jq >/dev/null 2>&1 && jq -r '.[].protoPayload.resourceName | capture("tables/(?<tid>[^/]+)$").tid' || cat) \
  | grep -v '^null$' | sort -u >> "$TMP_LIST" || true

  if [ -s "$TMP_LIST" ]; then
    while IFS= read -r line; do
      [ -n "$line" ] && TABLES+=( "$line" )
    done < <(sort -u "$TMP_LIST")
    info "Discovered ${#TABLES[@]} candidate tables from logs."
  else
    warn "No tables discovered from logs. Populate KNOWN_TABLES or use TABLES_CSV."
  fi
fi

if [ ${#TABLES[@]} -eq 0 ]; then
  err "No tables to restore. Add names to KNOWN_TABLES or TABLES_CSV, או ודא שלוגים זמינים."
  exit 4
fi

# ------------------------- RESTORE ------------------------------------
restore_one() {
  local t="$1"
  info "Restoring $t"

  # If target exists and FORCE!=1 → skip
  if bq --project_id="$PROJECT_ID" --location="$LOCATION" show "$PROJECT_ID:$TARGET_DATASET_ID.$t" >/dev/null 2>&1; then
    if [ "${FORCE:-}" = "1" ]; then
      warn "Target table exists; FORCE=1 → will replace: $t"
    else
      info "Target table exists; skipping: $t (set FORCE=1 to overwrite)"
      RESTORED_OK+=( "$t:skipped-exists" )
      return 0
    fi
  fi

  # 1) Fast path: copy via snapshot time decorator
  if [ -n "${SNAPSHOT_EPOCH_MS:-}" ] && [ "$SNAPSHOT_EPOCH_MS" != "0" ]; then
    if bq --project_id="$PROJECT_ID" --location="$LOCATION" cp \
      "$PROJECT_ID:$SOURCE_DATASET_ID.$t@$SNAPSHOT_EPOCH_MS" \
      "$PROJECT_ID:$TARGET_DATASET_ID.$t"; then
      info "✓ cp@epoch succeeded: $t"
      RESTORED_OK+=( "$t:cp" )
      return 0
    else
      warn "cp@epoch failed for $t; trying CTAS time-travel"
    fi
  else
    warn "SNAPSHOT_EPOCH_MS empty; skipping cp path for $t"
  fi

  # 2) Fallback: CTAS from time-travel query
  if bq --project_id="$PROJECT_ID" --location="$LOCATION" query --use_legacy_sql=false \
    "CREATE OR REPLACE TABLE \`$PROJECT_ID.$TARGET_DATASET_ID.$t\` AS
     SELECT * FROM \`$PROJECT_ID.$SOURCE_DATASET_ID.$t\`
     FOR SYSTEM_TIME AS OF TIMESTAMP('${SNAPSHOT_TIMESTAMP}');"; then
    info "✓ CTAS time-travel succeeded: $t"
    RESTORED_OK+=( "$t:ctas" )
    return 0
  else
    err "✗ Failed to restore table: $t"
    RESTORED_FAIL+=( "$t" )
    return 1
  fi
}

START_TS=$(date +%s)
for T in "${TABLES[@]}"; do
  [ -z "$T" ] && continue
  restore_one "$T" || true
done
END_TS=$(date +%s)

echo
info "Restore complete in $((END_TS-START_TS))s"
info "Succeeded: ${#RESTORED_OK[@]} table(s)"
for s in "${RESTORED_OK[@]}"; do echo "  - $s"; done
if [ ${#RESTORED_FAIL[@]} -gt 0 ]; then
  err "Failed: ${#RESTORED_FAIL[@]} table(s)"
  for f in "${RESTORED_FAIL[@]}"; do echo "  - $f"; done
  err "Check details in: $LOG_FILE"
  exit 5
fi

info "All done. Log saved to: $LOG_FILE"