#!/usr/bin/env python3
"""Debug script to check relationship detection."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from bigquery_to_erd.relationship_detector import RelationshipDetector
from bigquery_to_erd.schema_analyzer import SchemaAnalyzer
from bigquery_to_erd.bq_cli_connector import BQCLIConnector
from bigquery_to_erd.config import Config

def main():
    print("Loading configuration...")
    config_manager = Config()
    config = config_manager.get_erd_config()
    
    print("Connecting to BigQuery...")
    connector = BQCLIConnector(config)
    tables = connector.get_all_table_schemas()
    print(f"Found {len(tables)} tables")
    
    print("Analyzing tables...")
    analyzer = SchemaAnalyzer()
    analyzed_tables = []
    for table in tables:
        analyzed_table = analyzer.parse_table_schema(table)
        analyzed_tables.append(analyzed_table)
    print(f"Analyzed {len(analyzed_tables)} tables")
    
    print("Detecting relationships...")
    detector = RelationshipDetector()
    relationships = detector.detect_relationships(analyzed_tables)
    print(f"Detected {len(relationships)} relationships")
    
    if relationships:
        print("\nSample relationships:")
        for i, rel in enumerate(relationships[:5]):
            print(f"  {i+1}. {rel.source_table}.{rel.source_column} -> {rel.target_table}.{rel.target_column} (conf: {rel.confidence:.2f}, method: {rel.detection_method})")
        
        print("\nTable IDs:")
        for table in analyzed_tables:
            print(f"  - {table.table_id}")
        
        print("\nChecking relationship table matches...")
        table_ids = {table.table_id for table in analyzed_tables}
        for rel in relationships[:5]:
            source_match = rel.source_table in table_ids
            target_match = rel.target_table in table_ids
            print(f"  {rel.source_table} -> {rel.target_table}: source={source_match}, target={target_match}")
    else:
        print("No relationships detected!")

if __name__ == "__main__":
    main()
