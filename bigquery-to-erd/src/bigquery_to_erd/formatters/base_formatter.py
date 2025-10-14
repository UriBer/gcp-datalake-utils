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
        """Get position for table in improved layout.
        
        Args:
            table: Table schema
            index: Table index
            total_tables: Total number of tables
            
        Returns:
            Dictionary with x, y coordinates
        """
        # Improved grid layout with better spacing
        if total_tables <= 4:
            cols = total_tables
        elif total_tables <= 9:
            cols = 3
        else:
            cols = 4
        
        rows = (total_tables + cols - 1) // cols
        
        col = index % cols
        row = index // cols
        
        # Increased spacing for better readability
        spacing_x = 400
        spacing_y = 300
        
        # Center the layout
        total_width = (cols - 1) * spacing_x + 250
        total_height = (rows - 1) * spacing_y + 200
        
        start_x = max(50, (1200 - total_width) // 2)
        start_y = max(50, (800 - total_height) // 2)
        
        x = start_x + col * spacing_x
        y = start_y + row * spacing_y
        
        return {
            "x": x,
            "y": y
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
