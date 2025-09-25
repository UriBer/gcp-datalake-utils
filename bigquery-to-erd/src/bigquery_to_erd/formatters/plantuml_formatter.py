"""PlantUML formatter for ERD generation."""

from typing import List
from .base_formatter import BaseFormatter
from ..models import TableSchema, Relationship


class PlantUMLFormatter(BaseFormatter):
    """Formatter for PlantUML ERD format."""
    
    def format_erd(self, tables: List[TableSchema], 
                   relationships: List[Relationship]) -> str:
        """Format ERD data into PlantUML syntax.
        
        Args:
            tables: List of table schemas
            relationships: List of relationships
            
        Returns:
            PlantUML ERD string
        """
        self.validate_input(tables, relationships)
        
        lines = ["@startuml ERD"]
        lines.append("!theme plain")
        lines.append("")
        
        # Add table definitions
        for table in tables:
            lines.append(f"entity \"{table.table_id}\" as {self._get_entity_name(table.table_id)} {{")
            
            for column in table.columns:
                column_line = f"    {self._format_plantuml_column(column)}"
                lines.append(column_line)
            
            lines.append("}")
            lines.append("")  # Empty line between tables
        
        # Add relationships
        for relationship in relationships:
            rel_line = self._format_plantuml_relationship(relationship)
            lines.append(rel_line)
        
        lines.append("@enduml")
        
        return "\n".join(lines)
    
    def _get_entity_name(self, table_id: str) -> str:
        """Get PlantUML entity name from table ID.
        
        Args:
            table_id: Table ID
            
        Returns:
            PlantUML entity name
        """
        # Convert to valid PlantUML identifier
        return table_id.replace("-", "_").replace(" ", "_").lower()
    
    def _format_plantuml_column(self, column) -> str:
        """Format column for PlantUML.
        
        Args:
            column: Column info
            
        Returns:
            Formatted column string
        """
        parts = []
        
        # Add constraints
        if column.is_primary_key:
            parts.append("*")
        if column.is_foreign_key:
            parts.append("~")
        if column.mode == "REQUIRED":
            parts.append("NOT NULL")
        
        # Add column name
        parts.append(column.name)
        
        # Add data type
        if self.config.show_column_types:
            parts.append(": " + column.data_type)
        
        return " ".join(parts)
    
    def _format_plantuml_relationship(self, relationship: Relationship) -> str:
        """Format relationship for PlantUML.
        
        Args:
            relationship: Relationship object
            
        Returns:
            Formatted relationship string
        """
        source_entity = self._get_entity_name(relationship.source_table)
        target_entity = self._get_entity_name(relationship.target_table)
        
        # Map relationship types to PlantUML syntax
        type_mapping = {
            "one_to_one": "||--||",
            "one_to_many": "||--o{",
            "many_to_one": "}o--||",
            "many_to_many": "}o--o{"
        }
        
        connector = type_mapping.get(relationship.relationship_type.value, "||--o{")
        
        # Create relationship description
        description = f" : {relationship.source_column} -> {relationship.target_column}"
        
        return f"{source_entity} {connector} {target_entity}{description}"
    
    def get_file_extension(self) -> str:
        """Get file extension for PlantUML format.
        
        Returns:
            File extension
        """
        return ".puml"
