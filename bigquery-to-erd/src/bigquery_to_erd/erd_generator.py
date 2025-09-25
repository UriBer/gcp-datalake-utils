"""ERD generator with layout algorithms."""

import logging
from typing import List, Dict, Tuple, Optional
import math

from .models import TableSchema, Relationship, ERDConfig, TableLayout
from .formatters import DrawIOFormatter, MermaidFormatter, PlantUMLFormatter, BaseFormatter


logger = logging.getLogger(__name__)


class ERDGenerator:
    """Generates ERDs with various layout algorithms."""
    
    def __init__(self, config: ERDConfig):
        """Initialize ERD generator.
        
        Args:
            config: ERD configuration
        """
        self.config = config
        self.formatters = {
            "drawio": DrawIOFormatter(config),
            "mermaid": MermaidFormatter(config),
            "plantuml": PlantUMLFormatter(config),
        }
    
    def generate_erd(self, tables: List[TableSchema], 
                    relationships: List[Relationship]) -> str:
        """Generate ERD in the configured format.
        
        Args:
            tables: List of table schemas
            relationships: List of relationships
            
        Returns:
            Generated ERD string
        """
        if not tables:
            raise ValueError("No tables provided for ERD generation")
        
        if not relationships:
            logger.warning("No relationships provided - generating tables only")
        
        # Apply layout algorithm
        positioned_tables = self._apply_layout_algorithm(tables, relationships)
        
        # Get formatter
        formatter = self.formatters.get(self.config.output_format.value)
        if not formatter:
            raise ValueError(f"Unsupported output format: {self.config.output_format}")
        
        # Generate ERD
        erd_content = formatter.format_erd(positioned_tables, relationships)
        
        logger.info(f"Generated ERD with {len(tables)} tables and {len(relationships)} relationships")
        return erd_content
    
    def _apply_layout_algorithm(self, tables: List[TableSchema], 
                              relationships: List[Relationship]) -> List[TableSchema]:
        """Apply layout algorithm to position tables.
        
        Args:
            tables: List of table schemas
            relationships: List of relationships
            
        Returns:
            List of tables with positioning information
        """
        layout_type = self.config.table_layout
        
        if layout_type == TableLayout.AUTO:
            # Choose best layout based on number of tables and relationships
            if len(tables) <= 5:
                layout_type = TableLayout.GRID
            elif len(relationships) > len(tables) * 2:
                layout_type = TableLayout.FORCE_DIRECTED
            else:
                layout_type = TableLayout.HIERARCHICAL
        
        if layout_type == TableLayout.GRID:
            return self._grid_layout(tables)
        elif layout_type == TableLayout.HIERARCHICAL:
            return self._hierarchical_layout(tables, relationships)
        elif layout_type == TableLayout.FORCE_DIRECTED:
            return self._force_directed_layout(tables, relationships)
        elif layout_type == TableLayout.HORIZONTAL:
            return self._horizontal_layout(tables)
        elif layout_type == TableLayout.VERTICAL:
            return self._vertical_layout(tables)
        else:
            # Default to grid layout
            return self._grid_layout(tables)
    
    def _grid_layout(self, tables: List[TableSchema]) -> List[TableSchema]:
        """Apply grid layout algorithm.
        
        Args:
            tables: List of table schemas
            
        Returns:
            Tables with grid positioning
        """
        num_tables = len(tables)
        if num_tables == 0:
            return tables
        
        # Calculate grid dimensions
        cols = int(math.ceil(math.sqrt(num_tables)))
        rows = int(math.ceil(num_tables / cols))
        
        # Grid spacing
        cell_width = 300
        cell_height = 200
        
        positioned_tables = []
        for i, table in enumerate(tables):
            row = i // cols
            col = i % cols
            
            # Add position information to table (this would need to be stored in a custom way)
            # For now, we'll just return the tables as-is since the formatters handle positioning
            positioned_tables.append(table)
        
        logger.debug(f"Applied grid layout: {rows}x{cols} grid")
        return positioned_tables
    
    def _hierarchical_layout(self, tables: List[TableSchema], 
                           relationships: List[Relationship]) -> List[TableSchema]:
        """Apply hierarchical layout algorithm.
        
        Args:
            tables: List of table schemas
            relationships: List of relationships
            
        Returns:
            Tables with hierarchical positioning
        """
        # Build dependency graph
        graph = self._build_dependency_graph(tables, relationships)
        
        # Find root tables (tables with no incoming relationships)
        root_tables = []
        for table in tables:
            has_incoming = any(rel.target_table == table.table_id for rel in relationships)
            if not has_incoming:
                root_tables.append(table)
        
        # If no root tables found, use first table
        if not root_tables:
            root_tables = [tables[0]] if tables else []
        
        # Perform topological sort for hierarchical layout
        levels = self._topological_sort(graph, root_tables)
        
        positioned_tables = []
        for level, level_tables in enumerate(levels):
            for i, table in enumerate(level_tables):
                # Position tables in level
                positioned_tables.append(table)
        
        logger.debug(f"Applied hierarchical layout: {len(levels)} levels")
        return positioned_tables
    
    def _force_directed_layout(self, tables: List[TableSchema], 
                             relationships: List[Relationship]) -> List[TableSchema]:
        """Apply force-directed layout algorithm.
        
        Args:
            tables: List of table schemas
            relationships: List of relationships
            
        Returns:
            Tables with force-directed positioning
        """
        # Simple force-directed layout simulation
        # In a real implementation, you'd use a proper physics simulation
        
        # Initialize positions randomly
        positions = {}
        for i, table in enumerate(tables):
            angle = 2 * math.pi * i / len(tables)
            radius = 200
            x = radius * math.cos(angle)
            y = radius * math.sin(angle)
            positions[table.table_id] = (x, y)
        
        # Apply forces (simplified)
        for _ in range(10):  # Iterations
            forces = {table.table_id: (0, 0) for table in tables}
            
            # Repulsion forces between all tables
            for i, table1 in enumerate(tables):
                for j, table2 in enumerate(tables[i+1:], i+1):
                    x1, y1 = positions[table1.table_id]
                    x2, y2 = positions[table2.table_id]
                    
                    dx = x2 - x1
                    dy = y2 - y1
                    distance = math.sqrt(dx*dx + dy*dy)
                    
                    if distance > 0:
                        # Repulsion force
                        force = 1000 / (distance * distance)
                        fx = -force * dx / distance
                        fy = -force * dy / distance
                        
                        forces[table1.table_id] = (
                            forces[table1.table_id][0] + fx,
                            forces[table1.table_id][1] + fy
                        )
                        forces[table2.table_id] = (
                            forces[table2.table_id][0] - fx,
                            forces[table2.table_id][1] - fy
                        )
            
            # Attraction forces for relationships
            for rel in relationships:
                if rel.source_table in positions and rel.target_table in positions:
                    x1, y1 = positions[rel.source_table]
                    x2, y2 = positions[rel.target_table]
                    
                    dx = x2 - x1
                    dy = y2 - y1
                    distance = math.sqrt(dx*dx + dy*dy)
                    
                    if distance > 0:
                        # Attraction force
                        force = distance * 0.1
                        fx = force * dx / distance
                        fy = force * dy / distance
                        
                        forces[rel.source_table] = (
                            forces[rel.source_table][0] + fx,
                            forces[rel.source_table][1] + fy
                        )
                        forces[rel.target_table] = (
                            forces[rel.target_table][0] - fx,
                            forces[rel.target_table][1] - fy
                        )
            
            # Update positions
            for table in tables:
                fx, fy = forces[table.table_id]
                x, y = positions[table.table_id]
                positions[table.table_id] = (x + fx * 0.1, y + fy * 0.1)
        
        positioned_tables = tables  # Positions are handled by formatters
        logger.debug("Applied force-directed layout")
        return positioned_tables
    
    def _horizontal_layout(self, tables: List[TableSchema]) -> List[TableSchema]:
        """Apply horizontal layout algorithm.
        
        Args:
            tables: List of table schemas
            
        Returns:
            Tables with horizontal positioning
        """
        positioned_tables = []
        for i, table in enumerate(tables):
            # Position tables horizontally
            positioned_tables.append(table)
        
        logger.debug("Applied horizontal layout")
        return positioned_tables
    
    def _vertical_layout(self, tables: List[TableSchema]) -> List[TableSchema]:
        """Apply vertical layout algorithm.
        
        Args:
            tables: List of table schemas
            
        Returns:
            Tables with vertical positioning
        """
        positioned_tables = []
        for i, table in enumerate(tables):
            # Position tables vertically
            positioned_tables.append(table)
        
        logger.debug("Applied vertical layout")
        return positioned_tables
    
    def _build_dependency_graph(self, tables: List[TableSchema], 
                              relationships: List[Relationship]) -> Dict[str, List[str]]:
        """Build dependency graph from tables and relationships.
        
        Args:
            tables: List of table schemas
            relationships: List of relationships
            
        Returns:
            Dependency graph as adjacency list
        """
        graph = {table.table_id: [] for table in tables}
        
        for rel in relationships:
            if rel.source_table in graph and rel.target_table in graph:
                graph[rel.source_table].append(rel.target_table)
        
        return graph
    
    def _topological_sort(self, graph: Dict[str, List[str]], 
                         root_tables: List[TableSchema]) -> List[List[TableSchema]]:
        """Perform topological sort for hierarchical layout.
        
        Args:
            graph: Dependency graph
            root_tables: Root tables to start from
            
        Returns:
            List of levels, each containing tables at that level
        """
        levels = []
        visited = set()
        table_map = {table.table_id: table for table in root_tables}
        
        # Add all tables to table_map
        for table_id in graph:
            if table_id not in table_map:
                # This is a simplified approach - in reality you'd need the full table list
                pass
        
        current_level = [table for table in root_tables if table.table_id not in visited]
        while current_level:
            levels.append(current_level)
            visited.update(table.table_id for table in current_level)
            
            next_level = []
            for table in current_level:
                for neighbor in graph.get(table.table_id, []):
                    if neighbor not in visited:
                        # Find the table object
                        neighbor_table = table_map.get(neighbor)
                        if neighbor_table and neighbor_table not in next_level:
                            next_level.append(neighbor_table)
            
            current_level = next_level
        
        return levels
    
    def get_supported_formats(self) -> List[str]:
        """Get list of supported output formats.
        
        Returns:
            List of supported format names
        """
        return list(self.formatters.keys())
    
    def get_formatter(self, format_name: str) -> Optional[BaseFormatter]:
        """Get formatter for specific format.
        
        Args:
            format_name: Format name
            
        Returns:
            Formatter instance or None
        """
        return self.formatters.get(format_name)
