# Relationship Pattern Configuration

This directory contains configuration files for the BigQuery to ERD tool's relationship detection system.

## Files

- `relationship_patterns.json` - Main configuration file defining table patterns, naming conventions, and relationship detection rules

## Configuration Structure

### Table Patterns

The configuration supports multiple data warehouse methodologies:

#### Data Vault Patterns
- **Hubs (h_*)**: Business key containers
- **Dimensions (dim_*)**: Descriptive attributes
- **Links (l_*)**: Many-to-many relationships between hubs
- **References (ref_*)**: Lookup tables

#### Traditional Data Warehouse Patterns
- **Dimensions (dim_*)**: Dimension tables
- **Facts (fact_*)**: Fact tables with measures
- **Bridges (bridge_*)**: Bridge tables for many-to-many relationships

### Relationship Detection Strategies

1. **Exact Name Match**: Direct table name matching with suffix removal
2. **Pattern-Based Match**: Plural/singular transformations and data vault patterns
3. **Data Type Compatibility**: Matching based on compatible data types

### Column Patterns

- **Primary Key Indicators**: `id`, `key`, `hash_key`, `hk`, etc.
- **Foreign Key Indicators**: `*_id`, `*_hk`, `*_hash_key`, etc.

### Confidence Scoring

Different detection methods have different confidence scores:
- Exact match: 0.95
- Pattern match: 0.8
- Data vault pattern: 0.9
- Type compatibility: 0.6

### Filtering Rules

- Maximum relationships per table: 5
- Minimum confidence threshold: 0.3
- Preferred detection methods: `enhanced_pk_fk`, `foreign_key`, `data_vault_pattern`

## Usage

The configuration is automatically loaded by the `PatternConfigLoader` class. You can also specify a custom configuration file:

```python
from bigquery_to_erd.pattern_config import PatternConfigLoader

# Use default configuration
config = PatternConfigLoader()

# Use custom configuration
config = PatternConfigLoader("path/to/custom_config.json")
```

## Customization

To add new patterns or modify existing ones:

1. Edit `relationship_patterns.json`
2. Add new table patterns under `table_patterns`
3. Add new detection strategies under `relationship_detection.strategies`
4. Modify confidence scores and filtering rules as needed

## Examples

### Adding a New Data Vault Pattern

```json
{
  "table_patterns": {
    "data_vault": {
      "patterns": {
        "satellite": {
          "prefix": "s_",
          "description": "Satellite tables with descriptive attributes",
          "primary_key_patterns": ["id", "key", "sat_key", "sk"],
          "foreign_key_patterns": ["*_hk", "*_hash_key"],
          "relationship_rules": {
            "target_tables": ["h_*"],
            "relationship_type": "many_to_one",
            "confidence": 0.9
          }
        }
      }
    }
  }
}
```

### Adding a New Detection Strategy

```json
{
  "relationship_detection": {
    "strategies": [
      {
        "name": "custom_pattern_match",
        "description": "Custom pattern matching",
        "confidence": 0.85,
        "rules": [
          {
            "pattern": "custom_suffix_removal",
            "suffixes": ["_ref", "_reference"],
            "action": "remove_and_match"
          }
        ]
      }
    ]
  }
}
```
