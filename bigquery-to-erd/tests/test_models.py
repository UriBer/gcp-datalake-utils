"""Tests for data models."""

import pytest
from bigquery_to_erd.models import (
    ColumnInfo, TableSchema, Relationship, ERDConfig, 
    RelationshipType, OutputFormat, TableLayout
)


class TestColumnInfo:
    """Test ColumnInfo model."""
    
    def test_column_info_creation(self):
        """Test basic column info creation."""
        column = ColumnInfo(
            name="user_id",
            data_type="INTEGER",
            mode="REQUIRED"
        )
        
        assert column.name == "user_id"
        assert column.data_type == "INTEGER"
        assert column.mode == "REQUIRED"
        assert not column.is_primary_key
        assert not column.is_foreign_key
    
    def test_column_info_validation(self):
        """Test column info validation."""
        with pytest.raises(ValueError):
            ColumnInfo(
                name="test",
                data_type="INTEGER",
                mode="INVALID"
            )


class TestTableSchema:
    """Test TableSchema model."""
    
    def test_table_schema_creation(self):
        """Test basic table schema creation."""
        columns = [
            ColumnInfo(name="id", data_type="INTEGER", mode="REQUIRED"),
            ColumnInfo(name="name", data_type="STRING", mode="NULLABLE")
        ]
        
        table = TableSchema(
            table_id="users",
            dataset_id="test_dataset",
            project_id="test_project",
            columns=columns
        )
        
        assert table.table_id == "users"
        assert table.dataset_id == "test_dataset"
        assert table.project_id == "test_project"
        assert len(table.columns) == 2
        assert table.full_table_id == "test_project.test_dataset.users"
    
    def test_primary_keys_property(self):
        """Test primary keys property."""
        columns = [
            ColumnInfo(name="id", data_type="INTEGER", mode="REQUIRED", is_primary_key=True),
            ColumnInfo(name="name", data_type="STRING", mode="NULLABLE")
        ]
        
        table = TableSchema(
            table_id="users",
            dataset_id="test_dataset",
            project_id="test_project",
            columns=columns
        )
        
        primary_keys = table.primary_keys
        assert len(primary_keys) == 1
        assert primary_keys[0].name == "id"


class TestRelationship:
    """Test Relationship model."""
    
    def test_relationship_creation(self):
        """Test basic relationship creation."""
        relationship = Relationship(
            source_table="orders",
            source_column="customer_id",
            target_table="customers",
            target_column="id",
            relationship_type=RelationshipType.MANY_TO_ONE,
            confidence=0.8,
            detection_method="naming_convention"
        )
        
        assert relationship.source_table == "orders"
        assert relationship.target_table == "customers"
        assert relationship.relationship_type == RelationshipType.MANY_TO_ONE
        assert relationship.confidence == 0.8


class TestERDConfig:
    """Test ERDConfig model."""
    
    def test_erd_config_creation(self):
        """Test basic ERD config creation."""
        config = ERDConfig(
            project_id="test_project",
            dataset_id="test_dataset"
        )
        
        assert config.project_id == "test_project"
        assert config.dataset_id == "test_dataset"
        assert config.output_format == OutputFormat.DRAWIO
        assert config.table_layout == TableLayout.AUTO
    
    def test_erd_config_validation(self):
        """Test ERD config validation."""
        with pytest.raises(ValueError):
            ERDConfig(
                project_id="test_project",
                dataset_id="test_dataset",
                log_level="INVALID"
            )
