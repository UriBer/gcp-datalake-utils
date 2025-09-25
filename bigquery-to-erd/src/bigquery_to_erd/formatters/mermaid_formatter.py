"""Mermaid formatter for ERD generation."""

from typing import List
from .base_formatter import BaseFormatter
from ..models import TableSchema, Relationship


class MermaidFormatter(BaseFormatter):
    """Formatter for Mermaid ERD format."""
    
    def format_erd(self, tables: List[TableSchema], 
                   relationships: List[Relationship]) -> str:
        """Format ERD data into Mermaid syntax.
        
        Args:
            tables: List of table schemas
            relationships: List of relationships
            
        Returns:
            Mermaid ERD string
        """
        self.validate_input(tables, relationships)
        
        lines = ["erDiagram"]
        
        # Add table definitions
        for table in tables:
            lines.append(f"    {table.table_id} {{")
            
            for column in table.columns:
                column_line = f"        {self._format_mermaid_column(column)}"
                lines.append(column_line)
            
            lines.append("    }")
            lines.append("")  # Empty line between tables
        
        # Add relationships
        for relationship in relationships:
            rel_line = self._format_mermaid_relationship(relationship)
            lines.append(f"    {rel_line}")
        
        return "\n".join(lines)
    
    def _format_mermaid_column(self, column) -> str:
        """Format column for Mermaid.
        
        Args:
            column: Column info
            
        Returns:
            Formatted column string
        """
        parts = []
        
        # Add data type
        if self.config.show_column_types:
            parts.append(column.data_type.lower())
        else:
            parts.append("string")  # Default type
        
        # Add column name
        parts.append(column.name)
        
        # Add constraints
        if column.is_primary_key:
            parts.append("PK")
        if column.is_foreign_key:
            parts.append("FK")
        if column.mode == "REQUIRED":
            parts.append("NOT NULL")
        
        return " ".join(parts)
    
    def _format_mermaid_relationship(self, relationship: Relationship) -> str:
        """Format relationship for Mermaid.
        
        Args:
            relationship: Relationship object
            
        Returns:
            Formatted relationship string
        """
        source_table = relationship.source_table
        target_table = relationship.target_table
        
        # Map relationship types to Mermaid syntax
        type_mapping = {
            "one_to_one": "||--||",
            "one_to_many": "||--o{",
            "many_to_one": "}o--||",
            "many_to_many": "}o--o{"
        }
        
        connector = type_mapping.get(relationship.relationship_type.value, "||--o{")
        
        # Create relationship description
        description = f"{relationship.source_column} -> {relationship.target_column}"
        
        return f"{source_table} {connector} {target_table} : {description}"
    
    def get_file_extension(self) -> str:
        """Get file extension for Mermaid format.
        
        Returns:
            File extension
        """
        return ".mmd"
