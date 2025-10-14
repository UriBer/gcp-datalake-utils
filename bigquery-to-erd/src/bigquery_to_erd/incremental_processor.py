"""Incremental processing system for managing state and avoiding reprocessing."""

import json
import time
import hashlib
from pathlib import Path
from typing import Dict, List, Set, Optional, Any
import logging

from .models import Relationship, TableSchema

logger = logging.getLogger(__name__)


class IncrementalProcessor:
    """Manages incremental processing to avoid reprocessing unchanged data."""

    def __init__(self, state_file: str = "relationship_state.json"):
        """Initialize incremental processor.

        Args:
            state_file: Path to state persistence file
        """
        self.state_file = Path(state_file)
        self.processed_tables: Set[str] = set()
        self.relationship_graph: Dict[str, List[Relationship]] = {}
        self.table_checksums: Dict[str, str] = {}
        self.last_processed: Dict[str, float] = {}
        self.load_state()

    def load_state(self):
        """Load previous processing state from disk."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    self.processed_tables = set(state.get("processed_tables", []))
                    self.relationship_graph = {
                        table: [Relationship(**rel) for rel in relationships]
                        for table, relationships in state.get("relationship_graph", {}).items()
                    }
                    self.table_checksums = state.get("table_checksums", {})
                    self.last_processed = state.get("last_processed", {})
                
                logger.info(f"Loaded state: {len(self.processed_tables)} processed tables, "
                          f"{sum(len(rels) for rels in self.relationship_graph.values())} relationships")
            except Exception as e:
                logger.error(f"Error loading state from {self.state_file}: {e}")
                self._initialize_empty_state()

    def save_state(self):
        """Save current processing state to disk."""
        try:
            state = {
                "processed_tables": list(self.processed_tables),
                "relationship_graph": {
                    table: [rel.dict() for rel in relationships]
                    for table, relationships in self.relationship_graph.items()
                },
                "table_checksums": self.table_checksums,
                "last_processed": self.last_processed,
                "last_updated": time.time()
            }
            
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            logger.debug(f"Saved state to {self.state_file}")
        except Exception as e:
            logger.error(f"Error saving state to {self.state_file}: {e}")

    def _initialize_empty_state(self):
        """Initialize empty state."""
        self.processed_tables = set()
        self.relationship_graph = {}
        self.table_checksums = {}
        self.last_processed = {}

    def get_table_checksum(self, table: TableSchema) -> str:
        """Calculate checksum for a table to detect changes.

        Args:
            table: Table schema

        Returns:
            Checksum string
        """
        # Create a string representation of the table structure
        table_str = f"{table.table_id}:{table.project_id}:{table.dataset_id}"
        
        # Add column information
        columns_info = []
        for col in table.columns:
            col_info = f"{col.name}:{col.data_type}:{col.mode}:{col.is_primary_key}:{col.is_foreign_key}"
            columns_info.append(col_info)
        
        table_str += ":" + "|".join(sorted(columns_info))
        
        # Calculate MD5 hash
        return hashlib.md5(table_str.encode()).hexdigest()

    def is_table_changed(self, table: TableSchema) -> bool:
        """Check if a table has changed since last processing.

        Args:
            table: Table schema

        Returns:
            True if table has changed, False otherwise
        """
        current_checksum = self.get_table_checksum(table)
        stored_checksum = self.table_checksums.get(table.table_id)
        
        return current_checksum != stored_checksum

    def get_tables_to_process(self, all_tables: List[TableSchema]) -> List[TableSchema]:
        """Get list of tables that need processing.

        Args:
            all_tables: All available tables

        Returns:
            List of tables that need processing
        """
        tables_to_process = []
        
        for table in all_tables:
            # Check if table is new or changed
            if (table.table_id not in self.processed_tables or 
                self.is_table_changed(table)):
                tables_to_process.append(table)
                logger.debug(f"Table {table.table_id} needs processing (new: {table.table_id not in self.processed_tables}, "
                           f"changed: {self.is_table_changed(table)})")
        
        logger.info(f"Found {len(tables_to_process)} tables to process out of {len(all_tables)} total tables")
        return tables_to_process

    def get_existing_relationships(self, table_name: str) -> List[Relationship]:
        """Get existing relationships for a table.

        Args:
            table_name: Name of the table

        Returns:
            List of existing relationships
        """
        return self.relationship_graph.get(table_name, [])

    def update_table_relationships(self, table_name: str, relationships: List[Relationship]):
        """Update relationships for a table.

        Args:
            table_name: Name of the table
            relationships: List of relationships for the table
        """
        self.relationship_graph[table_name] = relationships
        self.last_processed[table_name] = time.time()
        logger.debug(f"Updated relationships for {table_name}: {len(relationships)} relationships")

    def mark_table_processed(self, table: TableSchema):
        """Mark a table as processed.

        Args:
            table: Table schema
        """
        self.processed_tables.add(table.table_id)
        self.table_checksums[table.table_id] = self.get_table_checksum(table)
        self.last_processed[table.table_id] = time.time()
        logger.debug(f"Marked table {table.table_id} as processed")

    def get_all_relationships(self) -> List[Relationship]:
        """Get all relationships from the relationship graph.

        Returns:
            List of all relationships
        """
        all_relationships = []
        for relationships in self.relationship_graph.values():
            all_relationships.extend(relationships)
        return all_relationships

    def get_relationship_stats(self) -> Dict[str, Any]:
        """Get statistics about processed relationships.

        Returns:
            Dictionary with relationship statistics
        """
        total_relationships = sum(len(rels) for rels in self.relationship_graph.values())
        processed_tables = len(self.processed_tables)
        
        # Calculate relationship types
        relationship_types = {}
        for relationships in self.relationship_graph.values():
            for rel in relationships:
                rel_type = rel.relationship_type
                relationship_types[rel_type] = relationship_types.get(rel_type, 0) + 1

        return {
            "total_relationships": total_relationships,
            "processed_tables": processed_tables,
            "relationship_types": relationship_types,
            "state_file": str(self.state_file),
            "last_updated": max(self.last_processed.values()) if self.last_processed else None
        }

    def clear_state(self, table_pattern: Optional[str] = None):
        """Clear processing state.

        Args:
            table_pattern: Optional pattern to match table names. If None, clears all.
        """
        if table_pattern:
            # Clear specific entries matching pattern
            tables_to_remove = [t for t in self.processed_tables if table_pattern in t]
            for table in tables_to_remove:
                self.processed_tables.discard(table)
                if table in self.relationship_graph:
                    del self.relationship_graph[table]
                if table in self.table_checksums:
                    del self.table_checksums[table]
                if table in self.last_processed:
                    del self.last_processed[table]
        else:
            # Clear all state
            self._initialize_empty_state()
        
        self.save_state()
        logger.info(f"Cleared state for pattern: {table_pattern or 'all'}")

    def is_stale(self, max_age_hours: float = 24) -> bool:
        """Check if the state is stale and needs refresh.

        Args:
            max_age_hours: Maximum age in hours before state is considered stale

        Returns:
            True if state is stale, False otherwise
        """
        if not self.last_processed:
            return True
        
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600
        
        # Check if any table was processed recently
        for last_time in self.last_processed.values():
            if current_time - last_time < max_age_seconds:
                return False
        
        return True
