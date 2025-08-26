# BigQuery Restore Runbook (he)

נוהל לשחזור טבלאות ב-BigQuery מתוך Time Travel כאשר:
- הדאטה־סט נמחק ונוצר דאטה־סט חדש באותו שם (UNDROP אינו אפשרי), או
- הדאטה־סט שוחזר אך ריק.

## דרישות מקדימות
- הרשאות:
  - BigQuery Data Viewer (מקור)
  - BigQuery Data Owner / Dataset Admin (יעד)
  - Logs Viewer (לגילוי שמות טבלאות דרך Cloud Audit Logs)
- כלים: `gcloud`, `bq`, `bash`, מומלץ `jq`, `python3`

## התקנה
```bash
git clone https://github.com/UriBer/gcp-datalake-utils.git
cd bigquery-restore
chmod +x restore_dataset.sh

# הרצה מהירה (כשמכירים את שמות הטבלאות)
TABLES_CSV="your_table_name1,msi_metadata" ./restore_dataset.sh
# Debug:
DEBUG=1 TABLES_CSV="your_table_name1,your_table_name2" ./restore_dataset.sh

אם לא הוגדר TABLES_CSV, הסקריפט ינסה לגלות שמות דרך Cloud Audit Logs בחלון של ±1 יום סביב SNAPSHOT_TIMESTAMP.
	•	אם רשימת הטבלאות לא נמצאה בלוגים → הגדירו TABLES_CSV ידנית.
	•	ודאו שלוגי Data Access מופעלים ל-BigQuery.

אם לא הוגדר TABLES_CSV, הסקריפט ינסה לגלות שמות דרך Cloud Audit Logs בחלון של ±1 יום סביב SNAPSHOT_TIMESTAMP.
	•	אם רשימת הטבלאות לא נמצאה בלוגים → הגדירו TABLES_CSV ידנית.
	•	ודאו שלוגי Data Access מופעלים ל-BigQuery.

מה הסקריפט עושה?
	1.	מאמת קיום דאטה־סט יעד ומזהה את המיקום (Region) האמיתי שלו.
	2.	מגלה שמות טבלאות מלוגים (או לפי TABLES_CSV/KNOWN_TABLES).
	3.	לכל טבלה:
	•	ניסיון 1: bq cp project:dataset.table@EPOCH_MS -> target (מהיר).
	•	ניסיון 2 (נפילה): CREATE OR REPLACE TABLE AS SELECT ... FOR SYSTEM_TIME AS OF (CTAS).
	4.	לוג מפורט נשמר ב־./logs/....
פתרון תקלות
	•	No tables to restore — לא נמצאו טבלאות בלוגים; שימוש ב-TABLES_CSV נחוץ.
	•	Not found in location ... — להריץ עם --location של הדאטה־סט המקורי (הסקריפט מזהה ומתאים).
	•	Already Exists: Dataset ... בשלב UNDROP (מחוץ לסקריפט) — סימן שקיים דאטה־סט בשם זה כרגע; UNDROP יחזיר את המחיקה האחרונה, לא ההיסטורית.
	•	בדקו את קובץ הלוג שנכתב ל־./logs/restore_<dataset>_<UTC>.log.

אבטחת איכות ומניעה
	•	קבעו גיבוי יומי ל-GCS (AVRO/ZSTD) + Snapshot Tables.
	•	צרו התראה ב-Cloud Logging על DatasetService.DeleteDataset ו-TableService.DeleteTable.
	•	הימנעו מיצירת דאטה־סט חדש באותו שם לפני ניסיון UNDROP.