#!/usr/bin/env python3
"""Test script to verify BigQuery to ERD installation."""

import sys
import importlib

def test_imports():
    """Test that all required modules can be imported."""
    modules = [
        'bigquery_to_erd',
        'bigquery_to_erd.models',
        'bigquery_to_erd.config',
        'bigquery_to_erd.bigquery_connector',
        'bigquery_to_erd.schema_analyzer',
        'bigquery_to_erd.relationship_detector',
        'bigquery_to_erd.erd_generator',
        'bigquery_to_erd.formatters',
        'bigquery_to_erd.formatters.drawio_formatter',
        'bigquery_to_erd.formatters.mermaid_formatter',
        'bigquery_to_erd.formatters.plantuml_formatter',
        'bigquery_to_erd.main',
    ]
    
    failed_imports = []
    
    for module in modules:
        try:
            importlib.import_module(module)
            print(f"✓ {module}")
        except ImportError as e:
            print(f"✗ {module}: {e}")
            failed_imports.append(module)
    
    return failed_imports

def test_models():
    """Test that models can be instantiated."""
    try:
        from bigquery_to_erd.models import (
            ColumnInfo, TableSchema, Relationship, ERDConfig,
            RelationshipType, OutputFormat, TableLayout
        )
        
        # Test ColumnInfo
        column = ColumnInfo(name="test", data_type="STRING")
        assert column.name == "test"
        print("✓ ColumnInfo model")
        
        # Test TableSchema
        table = TableSchema(
            table_id="test_table",
            dataset_id="test_dataset", 
            project_id="test_project"
        )
        assert table.table_id == "test_table"
        print("✓ TableSchema model")
        
        # Test Relationship
        relationship = Relationship(
            source_table="table1",
            source_column="col1",
            target_table="table2",
            target_column="col2",
            relationship_type=RelationshipType.MANY_TO_ONE,
            confidence=0.8,
            detection_method="test"
        )
        assert relationship.confidence == 0.8
        print("✓ Relationship model")
        
        # Test ERDConfig
        config = ERDConfig(project_id="test", dataset_id="test")
        assert config.output_format == OutputFormat.DRAWIO
        print("✓ ERDConfig model")
        
        return True
        
    except Exception as e:
        print(f"✗ Model tests failed: {e}")
        return False

def test_formatters():
    """Test that formatters can be instantiated."""
    try:
        from bigquery_to_erd.models import ERDConfig
        from bigquery_to_erd.formatters import (
            DrawIOFormatter, MermaidFormatter, PlantUMLFormatter
        )
        
        config = ERDConfig(project_id="test", dataset_id="test")
        
        # Test formatters
        drawio = DrawIOFormatter(config)
        assert drawio.get_file_extension() == ".drawio"
        print("✓ DrawIOFormatter")
        
        mermaid = MermaidFormatter(config)
        assert mermaid.get_file_extension() == ".mmd"
        print("✓ MermaidFormatter")
        
        plantuml = PlantUMLFormatter(config)
        assert plantuml.get_file_extension() == ".puml"
        print("✓ PlantUMLFormatter")
        
        return True
        
    except Exception as e:
        print(f"✗ Formatter tests failed: {e}")
        return False

def main():
    """Run all tests."""
    print("Testing BigQuery to ERD installation...")
    print("=" * 50)
    
    # Test imports
    print("\n1. Testing imports:")
    failed_imports = test_imports()
    
    if failed_imports:
        print(f"\n✗ {len(failed_imports)} imports failed")
        return False
    
    # Test models
    print("\n2. Testing models:")
    models_ok = test_models()
    
    # Test formatters
    print("\n3. Testing formatters:")
    formatters_ok = test_formatters()
    
    # Summary
    print("\n" + "=" * 50)
    if not failed_imports and models_ok and formatters_ok:
        print("✓ All tests passed! Installation is working correctly.")
        return True
    else:
        print("✗ Some tests failed. Check the output above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
