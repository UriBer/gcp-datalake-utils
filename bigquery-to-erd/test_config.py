#!/usr/bin/env python3
"""Test script to demonstrate the new configuration-based relationship detection."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from bigquery_to_erd.pattern_config import PatternConfigLoader
from bigquery_to_erd.schema_analyzer import SchemaAnalyzer
from bigquery_to_erd.relationship_detector import RelationshipDetector

def test_pattern_config():
    """Test the pattern configuration loader."""
    print("=== Testing Pattern Configuration ===")
    
    # Load configuration
    config_loader = PatternConfigLoader()
    
    # Test table pattern detection
    test_tables = [
        "h_adam",           # Data Vault Hub
        "dim_ishuv",        # Data Vault Dimension
        "l_adam_misgeret",  # Data Vault Link
        "ref_code_status",  # Data Vault Reference
        "fact_sales",       # Traditional Fact
        "dim_customer"      # Traditional Dimension
    ]
    
    print("\nTable Pattern Detection:")
    for table in test_tables:
        patterns = config_loader.get_patterns_for_table(table)
        print(f"  {table}: {[f'{methodology}.{pattern_name}' for methodology, pattern_name, _ in patterns]}")
    
    # Test primary key detection
    print("\nPrimary Key Detection:")
    test_columns = [
        ("id", "h_adam"),
        ("hash_key", "h_adam"),
        ("hk", "h_adam"),
        ("adam_id", "dim_ishuv"),
        ("dim_key", "dim_ishuv"),
        ("link_key", "l_adam_misgeret"),
        ("adam_hk", "l_adam_misgeret"),
        ("customer_id", "fact_sales")
    ]
    
    for column, table in test_columns:
        is_pk = config_loader.is_primary_key_candidate(column, table)
        is_fk = config_loader.is_foreign_key_candidate(column, table)
        print(f"  {table}.{column}: PK={is_pk}, FK={is_fk}")
    
    # Test target table finding
    print("\nTarget Table Finding:")
    available_tables = ["h_adam", "h_ishuv", "dim_ishuv", "l_adam_misgeret"]
    test_fk_columns = ["adam_hk", "ishuv_id", "adam_id", "hash_key"]
    
    for column in test_fk_columns:
        target = config_loader.find_target_table(column, available_tables)
        print(f"  {column} -> {target}")

def test_schema_analyzer():
    """Test the enhanced schema analyzer."""
    print("\n=== Testing Enhanced Schema Analyzer ===")
    
    # Create analyzer with configuration
    analyzer = SchemaAnalyzer()
    
    # Test primary key detection
    from bigquery_to_erd.models import ColumnInfo, TableSchema
    
    test_cases = [
        (ColumnInfo(name="id", data_type="INTEGER", is_nullable=False, mode="REQUIRED"), "h_adam"),
        (ColumnInfo(name="hash_key", data_type="STRING", is_nullable=False, mode="REQUIRED"), "h_adam"),
        (ColumnInfo(name="adam_id", data_type="INTEGER", is_nullable=False, mode="REQUIRED"), "dim_ishuv"),
        (ColumnInfo(name="link_key", data_type="STRING", is_nullable=False, mode="REQUIRED"), "l_adam_misgeret"),
    ]
    
    for column, table_name in test_cases:
        table_schema = TableSchema(
            table_id=table_name, 
            columns=[column],
            project_id="test-project",
            dataset_id="test-dataset"
        )
        is_pk = analyzer.identify_primary_key(column, table_schema)
        is_fk = analyzer.identify_foreign_key(column, table_schema)
        print(f"  {table_name}.{column.name}: PK={is_pk}, FK={is_fk}")

def test_relationship_detector():
    """Test the enhanced relationship detector."""
    print("\n=== Testing Enhanced Relationship Detector ===")
    
    # Create detector with configuration
    detector = RelationshipDetector()
    
    # Test filtering rules
    filtering_rules = detector.pattern_config.get_filtering_rules()
    print(f"  Max relationships per table: {filtering_rules.get('max_relationships_per_table', 'N/A')}")
    print(f"  Min confidence threshold: {filtering_rules.get('min_confidence_threshold', 'N/A')}")
    print(f"  Preferred methods: {filtering_rules.get('preferred_detection_methods', 'N/A')}")

if __name__ == "__main__":
    test_pattern_config()
    test_schema_analyzer()
    test_relationship_detector()
    print("\n=== Configuration-based system is working! ===")
