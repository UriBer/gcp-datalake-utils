"""Schema analyzer for processing BigQuery table schemas."""

import logging
import re
from typing import List, Dict, Set, Optional, Tuple
from collections import defaultdict

from .models import TableSchema, ColumnInfo, Relationship, RelationshipType


logger = logging.getLogger(__name__)


class SchemaAnalyzer:
    """Analyzer for BigQuery table schemas."""
    
    def __init__(self):
        """Initialize schema analyzer."""
        self.primary_key_patterns = [
            r'^id$',
            r'^.*_id$',
            r'^.*_key$',
            r'^.*_pk$',
            r'^pk_.*$',
        ]
        
        self.foreign_key_patterns = [
            r'^.*_id$',
            r'^.*_fk$',
            r'^.*_key$',
            r'^fk_.*$',
        ]
    
    def parse_table_schema(self, schema: TableSchema) -> TableSchema:
        """Parse and enhance table schema with additional information.
        
        Args:
            schema: TableSchema to parse
            
        Returns:
            Enhanced TableSchema with primary/foreign key detection
        """
        enhanced_columns = []
        
        for column in schema.columns:
            enhanced_column = self.extract_column_info(column, schema)
            enhanced_columns.append(enhanced_column)
        
        # Update schema with enhanced columns
        schema.columns = enhanced_columns
        return schema
    
    def extract_column_info(self, column: ColumnInfo, table_schema: TableSchema) -> ColumnInfo:
        """Extract additional column information.
        
        Args:
            column: Column to analyze
            table_schema: Parent table schema
            
        Returns:
            Enhanced ColumnInfo
        """
        # Detect primary keys
        is_primary_key = self.identify_primary_key(column, table_schema)
        
        # Detect foreign keys
        is_foreign_key = self.identify_foreign_key(column, table_schema)
        
        # Create enhanced column
        enhanced_column = ColumnInfo(
            name=column.name,
            data_type=column.data_type,
            mode=column.mode,
            description=column.description,
            is_primary_key=is_primary_key,
            is_foreign_key=is_foreign_key,
            max_length=column.max_length,
            precision=column.precision,
            scale=column.scale
        )
        
        return enhanced_column
    
    def identify_primary_keys(self, schema: TableSchema) -> List[ColumnInfo]:
        """Identify potential primary key columns.
        
        Args:
            schema: Table schema to analyze
            
        Returns:
            List of primary key columns
        """
        primary_keys = []
        
        for column in schema.columns:
            if self.identify_primary_key(column, schema):
                primary_keys.append(column)
        
        return primary_keys
    
    def identify_primary_key(self, column: ColumnInfo, table_schema: TableSchema) -> bool:
        """Check if a column is likely a primary key.
        
        Args:
            column: Column to check
            table_schema: Parent table schema
            
        Returns:
            True if column appears to be a primary key
        """
        # Check naming patterns
        for pattern in self.primary_key_patterns:
            if re.match(pattern, column.name, re.IGNORECASE):
                # Additional checks for primary key likelihood
                if self._is_primary_key_candidate(column, table_schema):
                    return True
        
        # Enhanced PK detection for data warehouse patterns
        if self._is_data_warehouse_primary_key(column, table_schema):
            return True
        
        return False
    
    def identify_foreign_key(self, column: ColumnInfo, table_schema: TableSchema) -> bool:
        """Check if a column is likely a foreign key.
        
        Args:
            column: Column to check
            table_schema: Parent table schema
            
        Returns:
            True if column appears to be a foreign key
        """
        # Check naming patterns
        for pattern in self.foreign_key_patterns:
            if re.match(pattern, column.name, re.IGNORECASE):
                # Additional checks for foreign key likelihood
                if self._is_foreign_key_candidate(column, table_schema):
                    return True
        
        return False
    
    def _is_primary_key_candidate(self, column: ColumnInfo, table_schema: TableSchema) -> bool:
        """Check if column is a good primary key candidate.
        
        Args:
            column: Column to check
            table_schema: Parent table schema
            
        Returns:
            True if good primary key candidate
        """
        # Primary keys should typically be:
        # - Required (not nullable)
        # - Integer or string type
        # - Not repeated
        
        if column.mode == "REPEATED":
            return False
        
        if column.mode == "NULLABLE" and column.name.lower() != "id":
            return False
        
        # Check data type
        numeric_types = ["INTEGER", "INT64", "STRING", "BYTES"]
        if column.data_type.upper() not in numeric_types:
            return False
        
        return True
    
    def _is_foreign_key_candidate(self, column: ColumnInfo, table_schema: TableSchema) -> bool:
        """Check if column is a good foreign key candidate.
        
        Args:
            column: Column to check
            table_schema: Parent table schema
            
        Returns:
            True if good foreign key candidate
        """
        # Foreign keys should typically be:
        # - Not the primary key of this table
        # - Integer or string type
        # - Not repeated
        
        if column.mode == "REPEATED":
            return False
        
        # Don't mark as FK if it's already identified as PK
        if self.identify_primary_key(column, table_schema):
            return False
        
        # Check data type
        numeric_types = ["INTEGER", "INT64", "STRING", "BYTES"]
        if column.data_type.upper() not in numeric_types:
            return False
        
        return True
    
    def get_table_relationships(self, tables: List[TableSchema]) -> List[Relationship]:
        """Get basic relationships between tables based on naming conventions.
        
        Args:
            tables: List of table schemas
            
        Returns:
            List of detected relationships
        """
        relationships = []
        table_map = {table.table_id: table for table in tables}
        
        for table in tables:
            for column in table.columns:
                if column.is_foreign_key:
                    # Try to find target table based on column name
                    target_table = self._find_target_table(column.name, table_map)
                    if target_table:
                        # Find target column (usually primary key)
                        target_column = self._find_target_column(target_table, column)
                        if target_column:
                            relationship = Relationship(
                                source_table=table.table_id,
                                source_column=column.name,
                                target_table=target_table.table_id,
                                target_column=target_column.name,
                                relationship_type=RelationshipType.MANY_TO_ONE,
                                confidence=0.7,  # Basic confidence for naming-based detection
                                detection_method="naming_convention"
                            )
                            relationships.append(relationship)
        
        return relationships
    
    def _find_target_table(self, column_name: str, table_map: Dict[str, TableSchema]) -> Optional[TableSchema]:
        """Find target table based on column name.
        
        Args:
            column_name: Foreign key column name
            table_map: Map of table_id to TableSchema
            
        Returns:
            Target table schema or None
        """
        # Common patterns for foreign key to table mapping
        patterns = [
            # user_id -> users
            (r'^(.+)_id$', lambda m: m.group(1) + 's'),
            # customer_id -> customers
            (r'^(.+)_id$', lambda m: m.group(1) + 's'),
            # order_id -> orders
            (r'^(.+)_id$', lambda m: m.group(1) + 's'),
        ]
        
        for pattern, transform in patterns:
            match = re.match(pattern, column_name, re.IGNORECASE)
            if match:
                target_name = transform(match)
                if target_name in table_map:
                    return table_map[target_name]
        
        return None
    
    def _find_target_column(self, target_table: TableSchema, source_column: ColumnInfo) -> Optional[ColumnInfo]:
        """Find target column in target table.
        
        Args:
            target_table: Target table schema
            source_column: Source column info
            
        Returns:
            Target column or None
        """
        # Look for primary key columns first
        primary_keys = [col for col in target_table.columns if col.is_primary_key]
        if primary_keys:
            return primary_keys[0]  # Return first primary key
        
        # Look for columns with matching data type and common names
        for column in target_table.columns:
            if (column.data_type == source_column.data_type and
                column.name.lower() in ['id', 'key', 'pk']):
                return column
        
        return None
    
    def analyze_schema_complexity(self, schema: TableSchema) -> Dict[str, any]:
        """Analyze schema complexity metrics.
        
        Args:
            schema: Table schema to analyze
            
        Returns:
            Dictionary with complexity metrics
        """
        metrics = {
            "total_columns": len(schema.columns),
            "primary_keys": len([c for c in schema.columns if c.is_primary_key]),
            "foreign_keys": len([c for c in schema.columns if c.is_foreign_key]),
            "nullable_columns": len([c for c in schema.columns if c.mode == "NULLABLE"]),
            "required_columns": len([c for c in schema.columns if c.mode == "REQUIRED"]),
            "repeated_columns": len([c for c in schema.columns if c.mode == "REPEATED"]),
            "data_types": len(set(c.data_type for c in schema.columns)),
            "has_description": len([c for c in schema.columns if c.description]) > 0,
            "table_size_mb": (schema.num_bytes or 0) / (1024 * 1024),
            "row_count": schema.num_rows or 0,
        }
    
    def _is_data_warehouse_primary_key(self, column: ColumnInfo, table_schema: TableSchema) -> bool:
        """Check if column is a data warehouse primary key.
        
        Args:
            column: Column to check
            table_schema: Parent table schema
            
        Returns:
            True if likely a data warehouse primary key
        """
        table_name = table_schema.table_id.lower()
        
        # Dimension table patterns
        if table_name.startswith('dim_'):
            # Look for surrogate keys
            if column.name.lower() in ['id', 'key', 'sk', 'surrogate_key']:
                return True
            
            # Look for business keys
            if column.name.lower().endswith('_id') and not column.name.lower().endswith('_fk'):
                return True
        
        # Fact table patterns
        elif table_name.startswith('fact_'):
            # Look for dimension foreign keys that could be part of composite PK
            if column.name.lower().endswith('_id') and not column.name.lower().endswith('_fk'):
                return True
        
        # Bridge table patterns
        elif table_name.startswith('bridge_') or table_name.startswith('l_'):
            # Look for relationship keys
            if column.name.lower() in ['id', 'key', 'relationship_id']:
                return True
        
        return False
