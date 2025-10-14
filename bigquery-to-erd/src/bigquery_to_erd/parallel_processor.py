"""Parallel processing system for relationship detection."""

import concurrent.futures
import logging
from typing import List, Dict, Any, Optional, Callable
from threading import Lock
from dataclasses import dataclass

from .models import TableSchema, Relationship

logger = logging.getLogger(__name__)


@dataclass
class ProcessingConfig:
    """Configuration for parallel processing."""
    max_workers: int = 4
    batch_size: int = 10
    enable_parallel: bool = True
    timeout_seconds: int = 300


class ParallelProcessor:
    """Handles parallel processing of relationship detection."""

    def __init__(self, config: Optional[ProcessingConfig] = None):
        """Initialize parallel processor.

        Args:
            config: Processing configuration
        """
        self.config = config or ProcessingConfig()
        self.results_lock = Lock()
        self.cache_lock = Lock()

    def process_tables_parallel(self, tables: List[TableSchema], 
                              process_func: Callable[[List[TableSchema]], List[Relationship]],
                              group_by_type: bool = True) -> List[Relationship]:
        """Process tables in parallel using the provided function.

        Args:
            tables: List of tables to process
            process_func: Function to process a group of tables
            group_by_type: Whether to group tables by type for better batching

        Returns:
            List of all detected relationships
        """
        if not self.config.enable_parallel or len(tables) < 2:
            logger.info("Processing tables sequentially")
            return process_func(tables)

        logger.info(f"Processing {len(tables)} tables in parallel with {self.config.max_workers} workers")

        # Group tables for better batching
        if group_by_type:
            table_groups = self._group_tables_by_type(tables)
        else:
            table_groups = self._group_tables_by_size(tables)

        all_relationships = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit tasks for each table group
            future_to_group = {
                executor.submit(self._process_group_with_timeout, group, process_func): group 
                for group in table_groups
            }

            # Collect results
            for future in concurrent.futures.as_completed(future_to_group):
                group = future_to_group[future]
                try:
                    relationships = future.result(timeout=self.config.timeout_seconds)
                    with self.results_lock:
                        all_relationships.extend(relationships)
                    logger.debug(f"Processed group with {len(group)} tables: {len(relationships)} relationships")
                except concurrent.futures.TimeoutError:
                    logger.error(f"Timeout processing group with tables: {[t.table_id for t in group]}")
                except Exception as e:
                    logger.error(f"Error processing group with tables {[t.table_id for t in group]}: {e}")

        logger.info(f"Parallel processing completed: {len(all_relationships)} total relationships")
        return all_relationships

    def _group_tables_by_type(self, tables: List[TableSchema]) -> List[List[TableSchema]]:
        """Group tables by their type (based on naming patterns).

        Args:
            tables: List of tables

        Returns:
            List of table groups
        """
        groups = {}
        
        for table in tables:
            table_type = self._get_table_type(table.table_id)
            if table_type not in groups:
                groups[table_type] = []
            groups[table_type].append(table)

        # Convert to list and ensure no group is too large
        table_groups = []
        for group in groups.values():
            if len(group) <= self.config.batch_size:
                table_groups.append(group)
            else:
                # Split large groups
                for i in range(0, len(group), self.config.batch_size):
                    table_groups.append(group[i:i + self.config.batch_size])

        logger.debug(f"Grouped {len(tables)} tables into {len(table_groups)} groups by type")
        return table_groups

    def _group_tables_by_size(self, tables: List[TableSchema]) -> List[List[TableSchema]]:
        """Group tables by size (number of columns).

        Args:
            tables: List of tables

        Returns:
            List of table groups
        """
        # Sort tables by column count
        sorted_tables = sorted(tables, key=lambda t: len(t.columns))
        
        groups = []
        current_group = []
        
        for table in sorted_tables:
            current_group.append(table)
            
            if len(current_group) >= self.config.batch_size:
                groups.append(current_group)
                current_group = []
        
        # Add remaining tables
        if current_group:
            groups.append(current_group)

        logger.debug(f"Grouped {len(tables)} tables into {len(groups)} groups by size")
        return groups

    def _get_table_type(self, table_name: str) -> str:
        """Get table type based on naming patterns.

        Args:
            table_name: Name of the table

        Returns:
            Table type string
        """
        table_name_lower = table_name.lower()
        
        if table_name_lower.startswith('h_'):
            return 'data_vault_hub'
        elif table_name_lower.startswith('dim_'):
            return 'data_vault_dimension'
        elif table_name_lower.startswith('l_'):
            return 'data_vault_link'
        elif table_name_lower.startswith('ref_'):
            return 'data_vault_reference'
        elif table_name_lower.startswith('fact_'):
            return 'fact_table'
        elif table_name_lower.startswith('bridge_'):
            return 'bridge_table'
        else:
            return 'other'

    def _process_group_with_timeout(self, table_group: List[TableSchema], 
                                  process_func: Callable[[List[TableSchema]], List[Relationship]]) -> List[Relationship]:
        """Process a group of tables with timeout handling.

        Args:
            table_group: Group of tables to process
            process_func: Function to process the tables

        Returns:
            List of relationships found
        """
        try:
            return process_func(table_group)
        except Exception as e:
            logger.error(f"Error in parallel processing: {e}")
            return []

    def process_relationships_parallel(self, tables: List[TableSchema], 
                                     relationship_detector,
                                     enable_fk_detection: bool = True,
                                     enable_naming_convention_detection: bool = True) -> List[Relationship]:
        """Process relationship detection in parallel.

        Args:
            tables: List of tables to process
            relationship_detector: Relationship detector instance
            enable_fk_detection: Whether to enable FK detection
            enable_naming_convention_detection: Whether to enable naming convention detection

        Returns:
            List of detected relationships
        """
        def process_tables_group(table_group: List[TableSchema]) -> List[Relationship]:
            """Process a group of tables for relationships."""
            return relationship_detector.detect_relationships(
                table_group,
                enable_fk_detection=enable_fk_detection,
                enable_naming_convention_detection=enable_naming_convention_detection
            )

        return self.process_tables_parallel(tables, process_tables_group)

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get parallel processing statistics.

        Returns:
            Dictionary with processing statistics
        """
        return {
            "max_workers": self.config.max_workers,
            "batch_size": self.config.batch_size,
            "parallel_enabled": self.config.enable_parallel,
            "timeout_seconds": self.config.timeout_seconds
        }
