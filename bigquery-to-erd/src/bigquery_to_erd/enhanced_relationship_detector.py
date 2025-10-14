"""Enhanced relationship detector with data testing, caching, and parallel processing."""

import logging
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from .models import TableSchema, Relationship
from .relationship_detector import RelationshipDetector
from .data_relationship_tester import DataRelationshipTester, DataTestResult
from .relationship_cache import RelationshipCache
from .incremental_processor import IncrementalProcessor
from .parallel_processor import ParallelProcessor, ProcessingConfig
from .pattern_config import PatternConfigLoader

logger = logging.getLogger(__name__)


class EnhancedRelationshipDetector:
    """Enhanced relationship detector with data testing, caching, and parallel processing."""

    def __init__(self, pattern_config_file: Optional[str] = None,
                 cache_dir: str = ".cache",
                 state_file: str = "relationship_state.json"):
        """Initialize enhanced relationship detector.

        Args:
            pattern_config_file: Path to pattern configuration file
            cache_dir: Directory for caching relationships
            state_file: Path to state persistence file
        """
        self.pattern_config = PatternConfigLoader(pattern_config_file)
        self.performance_config = self.pattern_config.get_performance_config()
        self.data_testing_config = self.pattern_config.get_data_testing_config()
        
        # Initialize components
        self.relationship_detector = RelationshipDetector(pattern_config_file=pattern_config_file)
        self.data_tester = DataRelationshipTester()
        self.cache = RelationshipCache(cache_dir) if self.performance_config.cache_enabled else None
        self.incremental_processor = IncrementalProcessor(state_file) if self.performance_config.incremental_processing else None
        
        # Configure parallel processing
        processing_config = ProcessingConfig(
            max_workers=self.performance_config.max_workers,
            batch_size=self.performance_config.batch_size,
            enable_parallel=self.performance_config.parallel_processing,
            timeout_seconds=self.performance_config.timeout_seconds
        )
        self.parallel_processor = ParallelProcessor(processing_config)

    def detect_relationships_enhanced(self, tables: List[TableSchema],
                                    enable_data_testing: bool = True,
                                    enable_parallel: bool = True) -> List[Relationship]:
        """Detect relationships with enhanced features.

        Args:
            tables: List of tables to process
            enable_data_testing: Whether to enable data-based testing
            enable_parallel: Whether to use parallel processing

        Returns:
            List of detected relationships
        """
        logger.info(f"Starting enhanced relationship detection for {len(tables)} tables")

        # Get tables to process (incremental processing)
        if self.incremental_processor:
            tables_to_process = self.incremental_processor.get_tables_to_process(tables)
            existing_relationships = self.incremental_processor.get_all_relationships()
        else:
            tables_to_process = tables
            existing_relationships = []

        if not tables_to_process:
            logger.info("No tables need processing (incremental processing)")
            return existing_relationships

        logger.info(f"Processing {len(tables_to_process)} tables")

        # Detect relationships
        if enable_parallel and self.performance_config.parallel_processing:
            new_relationships = self._detect_relationships_parallel(tables_to_process)
        else:
            new_relationships = self._detect_relationships_sequential(tables_to_process)

        # Apply data testing if enabled
        if enable_data_testing and self.data_testing_config.enabled:
            new_relationships = self._apply_data_testing(new_relationships, tables)

        # Filter relationships
        filtered_relationships = self._filter_relationships(new_relationships)

        # Update cache and state
        if self.cache:
            for relationship in filtered_relationships:
                self.cache.cache_relationship(relationship)

        if self.incremental_processor:
            for table in tables_to_process:
                table_relationships = [r for r in filtered_relationships 
                                    if r.source_table == table.table_id or r.target_table == table.table_id]
                self.incremental_processor.update_table_relationships(table.table_id, table_relationships)
                self.incremental_processor.mark_table_processed(table)
            self.incremental_processor.save_state()

        # Combine with existing relationships
        all_relationships = existing_relationships + filtered_relationships

        logger.info(f"Enhanced relationship detection completed: {len(all_relationships)} total relationships")
        return all_relationships

    def _detect_relationships_parallel(self, tables: List[TableSchema]) -> List[Relationship]:
        """Detect relationships using parallel processing."""
        logger.info("Using parallel processing for relationship detection")
        
        def process_tables_group(table_group: List[TableSchema]) -> List[Relationship]:
            return self.relationship_detector.detect_relationships(
                table_group,
                enable_fk_detection=True,
                enable_naming_convention_detection=True
            )

        return self.parallel_processor.process_tables_parallel(
            tables, 
            process_tables_group,
            group_by_type=self.performance_config.group_tables_by_type
        )

    def _detect_relationships_sequential(self, tables: List[TableSchema]) -> List[Relationship]:
        """Detect relationships using sequential processing."""
        logger.info("Using sequential processing for relationship detection")
        return self.relationship_detector.detect_relationships(
            tables,
            enable_fk_detection=True,
            enable_naming_convention_detection=True
        )

    def _apply_data_testing(self, relationships: List[Relationship], 
                          all_tables: List[TableSchema]) -> List[Relationship]:
        """Apply data-based testing to relationships.

        Args:
            relationships: List of relationships to test
            all_tables: List of all available tables

        Returns:
            List of relationships with updated confidence scores
        """
        if not relationships:
            return relationships

        logger.info(f"Applying data testing to {len(relationships)} relationships")

        # Create table lookup
        table_map = {table.table_id: table for table in all_tables}

        tested_relationships = []
        for relationship in relationships:
            # Check cache first
            if self.cache:
                cached_relationship = self.cache.get_cached_relationship(
                    relationship.source_table, relationship.target_table
                )
                if cached_relationship:
                    tested_relationships.append(cached_relationship)
                    continue

            # Get source and target tables
            source_table = table_map.get(relationship.source_table)
            target_table = table_map.get(relationship.target_table)

            if not source_table or not target_table:
                logger.warning(f"Missing table schema for relationship {relationship.source_table} -> {relationship.target_table}")
                tested_relationships.append(relationship)
                continue

            # Test relationship with data
            try:
                test_result = self.data_tester.test_relationship_with_data(
                    relationship, source_table, target_table,
                    sample_size=self.data_testing_config.sample_size
                )

                # Update relationship confidence based on data testing
                if test_result.overall_confidence >= self.data_testing_config.confidence_threshold:
                    # Boost confidence for data-validated relationships
                    updated_confidence = min(1.0, relationship.confidence + 0.2)
                    relationship.confidence = updated_confidence
                    relationship.relationship_type = f"{relationship.relationship_type}_data_validated"
                    
                    logger.debug(f"Data testing passed for {relationship.source_table} -> {relationship.target_table}: "
                               f"confidence={updated_confidence:.3f}")
                else:
                    # Reduce confidence for failed data testing
                    updated_confidence = max(0.1, relationship.confidence - 0.3)
                    relationship.confidence = updated_confidence
                    
                    logger.debug(f"Data testing failed for {relationship.source_table} -> {relationship.target_table}: "
                               f"confidence={updated_confidence:.3f}")

                tested_relationships.append(relationship)

            except Exception as e:
                logger.error(f"Error in data testing for {relationship.source_table} -> {relationship.target_table}: {e}")
                tested_relationships.append(relationship)

        return tested_relationships

    def _filter_relationships(self, relationships: List[Relationship]) -> List[Relationship]:
        """Filter relationships based on configuration rules.

        Args:
            relationships: List of relationships to filter

        Returns:
            Filtered list of relationships
        """
        if not relationships:
            return relationships

        filtering_rules = self.pattern_config.get_filtering_rules()
        max_rels_per_table = filtering_rules.get("max_relationships_per_table", 5)
        min_confidence = filtering_rules.get("min_confidence_threshold", 0.3)
        preferred_methods = filtering_rules.get("preferred_detection_methods", [])

        # Group relationships by source table
        table_relationships = {}
        for rel in relationships:
            if rel.source_table not in table_relationships:
                table_relationships[rel.source_table] = []
            table_relationships[rel.source_table].append(rel)

        filtered_relationships = []
        for table, rels in table_relationships.items():
            # Sort by confidence and preferred methods
            rels.sort(key=lambda r: (
                r.detection_method in preferred_methods,
                r.confidence
            ), reverse=True)

            # Take top relationships up to max_rels_per_table
            table_filtered = []
            for rel in rels:
                if len(table_filtered) >= max_rels_per_table:
                    break
                if rel.confidence >= min_confidence:
                    table_filtered.append(rel)

            filtered_relationships.extend(table_filtered)

        logger.info(f"Filtered relationships: {len(relationships)} -> {len(filtered_relationships)}")
        return filtered_relationships

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics.

        Returns:
            Dictionary with processing statistics
        """
        stats = {
            "parallel_processing": self.parallel_processor.get_processing_stats(),
            "data_testing_enabled": self.data_testing_config.enabled,
            "cache_enabled": self.cache is not None,
            "incremental_processing": self.incremental_processor is not None
        }

        if self.cache:
            stats["cache_stats"] = self.cache.get_cache_stats()

        if self.incremental_processor:
            stats["incremental_stats"] = self.incremental_processor.get_relationship_stats()

        return stats

    def clear_cache(self, table_pattern: Optional[str] = None):
        """Clear relationship cache.

        Args:
            table_pattern: Optional pattern to match table names
        """
        if self.cache:
            self.cache.clear_cache(table_pattern)
        if self.incremental_processor:
            self.incremental_processor.clear_state(table_pattern)

    def get_relationship_quality_report(self, relationships: List[Relationship]) -> Dict[str, Any]:
        """Generate a quality report for relationships.

        Args:
            relationships: List of relationships to analyze

        Returns:
            Dictionary with quality metrics
        """
        if not relationships:
            return {"total_relationships": 0}

        # Calculate quality metrics
        total_relationships = len(relationships)
        high_confidence = len([r for r in relationships if r.confidence >= 0.8])
        medium_confidence = len([r for r in relationships if 0.5 <= r.confidence < 0.8])
        low_confidence = len([r for r in relationships if r.confidence < 0.5])

        # Group by detection method
        by_method = {}
        for rel in relationships:
            method = rel.detection_method
            by_method[method] = by_method.get(method, 0) + 1

        # Group by relationship type
        by_type = {}
        for rel in relationships:
            rel_type = rel.relationship_type
            by_type[rel_type] = by_type.get(rel_type, 0) + 1

        return {
            "total_relationships": total_relationships,
            "confidence_distribution": {
                "high_confidence": high_confidence,
                "medium_confidence": medium_confidence,
                "low_confidence": low_confidence
            },
            "by_detection_method": by_method,
            "by_relationship_type": by_type,
            "average_confidence": sum(r.confidence for r in relationships) / total_relationships
        }
