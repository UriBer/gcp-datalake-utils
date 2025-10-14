"""Base formatter class for ERD output formats."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from ..models import TableSchema, Relationship, ERDConfig


class BaseFormatter(ABC):
    """Base class for ERD formatters."""
    
    def __init__(self, config: ERDConfig):
        """Initialize formatter.
        
        Args:
            config: ERD configuration
        """
        self.config = config
    
    @abstractmethod
    def format_erd(self, tables: List[TableSchema], 
                   relationships: List[Relationship]) -> str:
        """Format ERD data into output string.
        
        Args:
            tables: List of table schemas
            relationships: List of relationships
            
        Returns:
            Formatted ERD string
        """
        pass
    
    @abstractmethod
    def get_file_extension(self) -> str:
        """Get file extension for this format.
        
        Returns:
            File extension (e.g., '.drawio', '.mmd')
        """
        pass
    
    def validate_input(self, tables: List[TableSchema], 
                      relationships: List[Relationship]) -> None:
        """Validate input data.
        
        Args:
            tables: List of table schemas
            relationships: List of relationships
            
        Raises:
            ValueError: If input data is invalid
        """
        if not tables:
            raise ValueError("No tables provided")
        
        # Relationships are optional - we can generate ERD with tables only
        # if not relationships:
        #     raise ValueError("No relationships provided")
        
        # Check for duplicate table IDs
        table_ids = [table.table_id for table in tables]
        if len(table_ids) != len(set(table_ids)):
            raise ValueError("Duplicate table IDs found")
    
    def get_table_position(self, table: TableSchema, index: int, 
                          total_tables: int) -> Dict[str, float]:
        """Get position for table in layout.
        
        Args:
            table: Table schema
            index: Table index
            total_tables: Total number of tables
            
        Returns:
            Dictionary with x, y coordinates
        """
        # Simple grid layout
        cols = int(total_tables ** 0.5) + 1
        row = index // cols
        col = index % cols
        
        return {
            "x": col * 300,
            "y": row * 200
        }
    
    def format_column_info(self, column) -> str:
        """Format column information for display.
        
        Args:
            column: Column info
            
        Returns:
            Formatted column string
        """
        parts = [column.name]
        
        if self.config.show_column_types:
            parts.append(f"({column.data_type})")
        
        if self.config.show_column_nullable and column.mode == "NULLABLE":
            parts.append("NULL")
        
        return " ".join(parts)
    
    def get_relationship_label(self, relationship) -> str:
        """Get label for relationship.
        
        Args:
            relationship: Relationship object
            
        Returns:
            Relationship label
        """
        type_labels = {
            "one_to_one": "1:1",
            "one_to_many": "1:N", 
            "many_to_one": "N:1",
            "many_to_many": "N:N"
        }
        
        return type_labels.get(relationship.relationship_type.value, "?")
