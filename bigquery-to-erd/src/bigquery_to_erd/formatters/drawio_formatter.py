"""Draw.io XML formatter for ERD generation."""

import xml.etree.ElementTree as ET
from typing import List, Dict, Any
from xml.dom import minidom

from .base_formatter import BaseFormatter
from ..models import TableSchema, Relationship


class DrawIOFormatter(BaseFormatter):
    """Formatter for Draw.io XML format."""
    
    def __init__(self, config):
        """Initialize Draw.io formatter.
        
        Args:
            config: ERD configuration
        """
        super().__init__(config)
        self.cell_id_counter = 2  # Start from 2 to avoid conflicts with standard IDs (0, 1)
    
    def format_erd(self, tables: List[TableSchema], 
                   relationships: List[Relationship]) -> str:
        """Format ERD data into Draw.io XML.
        
        Args:
            tables: List of table schemas
            relationships: List of relationships
            
        Returns:
            Draw.io XML string
        """
        self.validate_input(tables, relationships)
        
        # Create root element
        root = ET.Element("mxfile")
        root.set("host", "app.diagrams.net")
        root.set("modified", "2023-01-01T00:00:00.000Z")
        root.set("agent", "bigquery-to-erd")
        root.set("version", "22.1.16")
        root.set("etag", "abc123")
        root.set("type", "device")
        
        # Create diagram
        diagram = ET.SubElement(root, "diagram")
        diagram.set("id", "erd-diagram")
        diagram.set("name", f"ERD - {self.config.dataset_id}")
        
        # Create mxGraphModel
        mxgraph = ET.SubElement(diagram, "mxGraphModel")
        mxgraph.set("dx", "1422")
        mxgraph.set("dy", "754")
        mxgraph.set("grid", "1")
        mxgraph.set("gridSize", "10")
        mxgraph.set("guides", "1")
        mxgraph.set("tooltips", "1")
        mxgraph.set("connect", "1")
        mxgraph.set("arrows", "1")
        mxgraph.set("fold", "1")
        mxgraph.set("page", "1")
        mxgraph.set("pageScale", "1")
        mxgraph.set("pageWidth", "1169")
        mxgraph.set("pageHeight", "827")
        mxgraph.set("math", "0")
        mxgraph.set("shadow", "0")
        
        # Create root cell
        root_cell = ET.SubElement(mxgraph, "root")
        
        # Create parent cell
        parent_cell = ET.SubElement(root_cell, "mxCell")
        parent_cell.set("id", "0")
        
        # Create default parent cell
        default_parent = ET.SubElement(root_cell, "mxCell")
        default_parent.set("id", "1")
        default_parent.set("parent", "0")
        
        # Create table cells
        table_cells = {}
        for i, table in enumerate(tables):
            table_cell = self._create_table_cell(table, i, len(tables))
            table_cells[table.table_id] = table_cell
            root_cell.append(table_cell)
        
        # Create relationship cells
        for relationship in relationships:
            if (relationship.source_table in table_cells and 
                relationship.target_table in table_cells):
                edge_cell = self._create_relationship_cell(
                    relationship, table_cells[relationship.source_table],
                    table_cells[relationship.target_table]
                )
                root_cell.append(edge_cell)
        
        # Convert to string
        rough_string = ET.tostring(root, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="  ")
    
    def _create_table_cell(self, table: TableSchema, index: int, 
                          total_tables: int) -> ET.Element:
        """Create a table cell for Draw.io.
        
        Args:
            table: Table schema
            index: Table index
            total_tables: Total number of tables
            
        Returns:
            Table cell element
        """
        position = self.get_table_position(table, index, total_tables)
        
        # Calculate table dimensions
        num_columns = len(table.columns)
        table_width = 200
        table_height = 50 + (num_columns * 25)
        
        # Create table cell
        table_cell = ET.Element("mxCell")
        table_cell.set("id", str(self._get_next_cell_id()))
        table_cell.set("value", self._format_table_value(table))
        table_cell.set("style", self._get_table_style())
        table_cell.set("vertex", "1")
        table_cell.set("parent", "1")
        
        # Set geometry
        geometry = ET.SubElement(table_cell, "mxGeometry")
        geometry.set("x", str(position["x"]))
        geometry.set("y", str(position["y"]))
        geometry.set("width", str(table_width))
        geometry.set("height", str(table_height))
        geometry.set("as", "geometry")
        
        return table_cell
    
    def _create_relationship_cell(self, relationship: Relationship,
                                source_cell: ET.Element, 
                                target_cell: ET.Element) -> ET.Element:
        """Create a relationship edge cell.
        
        Args:
            relationship: Relationship object
            source_cell: Source table cell
            target_cell: Target table cell
            
        Returns:
            Edge cell element
        """
        edge_cell = ET.Element("mxCell")
        edge_cell.set("id", str(self._get_next_cell_id()))
        edge_cell.set("value", self.get_relationship_label(relationship))
        edge_cell.set("style", self._get_edge_style(relationship))
        edge_cell.set("edge", "1")
        edge_cell.set("parent", "1")
        edge_cell.set("source", source_cell.get("id"))
        edge_cell.set("target", target_cell.get("id"))
        
        # Set geometry
        geometry = ET.SubElement(edge_cell, "mxGeometry")
        geometry.set("relative", "1")
        geometry.set("as", "geometry")
        
        return edge_cell
    
    def _format_table_value(self, table: TableSchema) -> str:
        """Format table value for Draw.io.
        
        Args:
            table: Table schema
            
        Returns:
            Formatted table value
        """
        lines = [f"<b>{table.table_id}</b>"]
        
        for column in table.columns:
            column_info = self.format_column_info(column)
            if column.is_primary_key:
                column_info = f"<b>{column_info}</b>"
            if column.is_foreign_key:
                column_info = f"<i>{column_info}</i>"
            lines.append(column_info)
        
        return "<br/>".join(lines)
    
    def _get_table_style(self) -> str:
        """Get table cell style.
        
        Returns:
            Style string
        """
        base_style = "swimlane;fontStyle=1;align=center;verticalAlign=top;childLayout=stackLayout;horizontal=1;startSize=30;horizontalStack=0;resizeParent=1;resizeParentMax=0;resizeLast=0;collapsible=1;marginBottom=0;"
        
        if self.config.drawio_theme == "dark":
            base_style += "fillColor=#2d2d2d;strokeColor=#666666;fontColor=#ffffff;"
        elif self.config.drawio_theme == "minimal":
            base_style += "fillColor=#ffffff;strokeColor=#000000;fontColor=#000000;"
        else:  # default
            base_style += "fillColor=#dae8fc;strokeColor=#6c8ebf;fontColor=#000000;"
        
        return base_style
    
    def _get_edge_style(self, relationship: Relationship) -> str:
        """Get edge style for relationship.
        
        Args:
            relationship: Relationship object
            
        Returns:
            Style string
        """
        base_style = "endArrow=classic;html=1;rounded=0;"
        
        # Add relationship type styling
        if relationship.relationship_type.value == "one_to_one":
            base_style += "startArrow=classic;"
        elif relationship.relationship_type.value == "one_to_many":
            base_style += "startArrow=none;"
        elif relationship.relationship_type.value == "many_to_one":
            base_style += "startArrow=none;"
        elif relationship.relationship_type.value == "many_to_many":
            base_style += "startArrow=classic;"
        
        # Add confidence-based styling
        if relationship.confidence < 0.5:
            base_style += "strokeColor=#ff9999;strokeWidth=2;"
        elif relationship.confidence < 0.8:
            base_style += "strokeColor=#ffcc99;strokeWidth=2;"
        else:
            base_style += "strokeColor=#99cc99;strokeWidth=2;"
        
        return base_style
    
    def _get_next_cell_id(self) -> int:
        """Get next cell ID.
        
        Returns:
            Next cell ID
        """
        cell_id = self.cell_id_counter
        self.cell_id_counter += 1
        # Ensure we never use standard Draw.io IDs (0, 1)
        if cell_id <= 1:
            cell_id = self.cell_id_counter
            self.cell_id_counter += 1
        return cell_id
    
    def get_file_extension(self) -> str:
        """Get file extension for Draw.io format.
        
        Returns:
            File extension
        """
        return ".drawio"
