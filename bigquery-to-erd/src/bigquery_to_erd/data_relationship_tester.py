"""Data-based relationship testing for validating relationships with actual data."""

import logging
import math
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

from .models import Relationship, TableSchema, ColumnInfo
from .bigquery_connector import BigQueryConnector
from .bq_cli_connector import BQCLIConnector

logger = logging.getLogger(__name__)


@dataclass
class DataTestResult:
    """Result of data-based relationship testing."""
    referential_integrity: float
    type_compatibility: float
    distribution_similarity: float
    overall_confidence: float
    sample_size: int
    orphan_count: int
    total_source_records: int


class DataRelationshipTester:
    """Tests relationships between tables based on actual data analysis."""

    def __init__(self, connector: Optional[BigQueryConnector] = None, 
                 bq_cli_connector: Optional[BQCLIConnector] = None):
        """Initialize data relationship tester.

        Args:
            connector: BigQuery Python client connector
            bq_cli_connector: BigQuery CLI connector as fallback
        """
        self.connector = connector
        self.bq_cli_connector = bq_cli_connector
        self.sample_cache = {}

    def test_relationship_with_data(self, relationship: Relationship, 
                                  source_table: TableSchema,
                                  target_table: TableSchema,
                                  sample_size: int = 1000) -> DataTestResult:
        """Test relationship by analyzing actual data samples.

        Args:
            relationship: Relationship to test
            source_table: Source table schema
            target_table: Target table schema
            sample_size: Number of records to sample for testing

        Returns:
            DataTestResult with test scores
        """
        logger.info(f"Testing relationship {relationship.source_table} -> {relationship.target_table} with data")

        # Get sample data from both tables
        source_sample = self._get_sample_data(
            relationship.source_table, 
            relationship.source_column, 
            sample_size
        )
        target_sample = self._get_sample_data(
            relationship.target_table, 
            relationship.target_column, 
            sample_size
        )

        if not source_sample or not target_sample:
            logger.warning(f"Insufficient data for testing relationship {relationship.source_table} -> {relationship.target_table}")
            return DataTestResult(0.0, 0.0, 0.0, 0.0, 0, 0, 0)

        # Calculate test metrics
        referential_integrity = self._calculate_referential_integrity(source_sample, target_sample)
        type_compatibility = self._check_type_compatibility(source_table, target_table, relationship)
        distribution_similarity = self._compare_value_distributions(source_sample, target_sample)
        
        # Calculate overall confidence
        overall_confidence = self._calculate_overall_confidence(
            referential_integrity, type_compatibility, distribution_similarity
        )

        return DataTestResult(
            referential_integrity=referential_integrity,
            type_compatibility=type_compatibility,
            distribution_similarity=distribution_similarity,
            overall_confidence=overall_confidence,
            sample_size=len(source_sample),
            orphan_count=len(source_sample) - len(set(source_sample).intersection(set(target_sample))),
            total_source_records=len(source_sample)
        )

    def _get_sample_data(self, table_name: str, column_name: str, limit: int) -> List[Any]:
        """Get sample data from a table column.

        Args:
            table_name: Name of the table
            column_name: Name of the column
            limit: Maximum number of records to sample

        Returns:
            List of sample values
        """
        cache_key = f"{table_name}_{column_name}_{limit}"
        
        # Check cache first
        if cache_key in self.sample_cache:
            return self.sample_cache[cache_key]

        try:
            # Try Python client first
            if self.connector:
                query = f"""
                SELECT {column_name}
                FROM `{table_name}`
                WHERE {column_name} IS NOT NULL
                LIMIT {limit}
                """
                result = self.connector.client.query(query).result()
                sample_data = [row[0] for row in result]
            else:
                # Fallback to BQ CLI
                sample_data = self._get_sample_data_via_cli(table_name, column_name, limit)

            # Cache the result
            self.sample_cache[cache_key] = sample_data
            return sample_data

        except Exception as e:
            logger.error(f"Error getting sample data from {table_name}.{column_name}: {e}")
            return []

    def _get_sample_data_via_cli(self, table_name: str, column_name: str, limit: int) -> List[Any]:
        """Get sample data using BQ CLI as fallback."""
        if not self.bq_cli_connector:
            return []

        try:
            query = f"""
            SELECT {column_name}
            FROM `{table_name}`
            WHERE {column_name} IS NOT NULL
            LIMIT {limit}
            """
            result = self.bq_cli_connector._execute_bq_query(query)
            return [row[0] for row in result] if result else []
        except Exception as e:
            logger.error(f"Error getting sample data via CLI from {table_name}.{column_name}: {e}")
            return []

    def _calculate_referential_integrity(self, source_values: List[Any], target_values: List[Any]) -> float:
        """Calculate referential integrity score.

        Args:
            source_values: Values from source table
            target_values: Values from target table

        Returns:
            Referential integrity score (0.0 to 1.0)
        """
        if not source_values:
            return 0.0

        source_set = set(source_values)
        target_set = set(target_values)
        
        # Calculate overlap
        overlap = len(source_set.intersection(target_set))
        total_source = len(source_set)
        
        # Referential integrity is the percentage of source values that exist in target
        integrity = overlap / total_source if total_source > 0 else 0.0
        
        logger.debug(f"Referential integrity: {integrity:.3f} ({overlap}/{total_source})")
        return integrity

    def _check_type_compatibility(self, source_table: TableSchema, target_table: TableSchema, 
                                 relationship: Relationship) -> float:
        """Check data type compatibility between source and target columns.

        Args:
            source_table: Source table schema
            target_table: Target table schema
            relationship: Relationship being tested

        Returns:
            Type compatibility score (0.0 to 1.0)
        """
        # Find source and target columns
        source_column = None
        target_column = None

        for col in source_table.columns:
            if col.name == relationship.source_column:
                source_column = col
                break

        for col in target_table.columns:
            if col.name == relationship.target_column:
                target_column = col
                break

        if not source_column or not target_column:
            return 0.0

        # Check if data types are compatible
        source_type = source_column.data_type.lower()
        target_type = target_column.data_type.lower()

        # Exact match
        if source_type == target_type:
            return 1.0

        # Compatible types
        compatible_types = {
            'int64': ['integer', 'int32', 'int64'],
            'integer': ['int64', 'int32', 'integer'],
            'string': ['varchar', 'text', 'char'],
            'varchar': ['string', 'text', 'char'],
            'float64': ['float', 'double', 'numeric'],
            'float': ['float64', 'double', 'numeric'],
            'timestamp': ['datetime', 'date'],
            'datetime': ['timestamp', 'date']
        }

        if source_type in compatible_types and target_type in compatible_types[source_type]:
            return 0.8

        # Numeric types are generally compatible
        numeric_types = ['int64', 'integer', 'int32', 'float64', 'float', 'double', 'numeric']
        if source_type in numeric_types and target_type in numeric_types:
            return 0.6

        # String types are generally compatible
        string_types = ['string', 'varchar', 'text', 'char']
        if source_type in string_types and target_type in string_types:
            return 0.6

        return 0.2  # Low compatibility for very different types

    def _compare_value_distributions(self, source_values: List[Any], target_values: List[Any]) -> float:
        """Compare value distributions between source and target.

        Args:
            source_values: Values from source table
            target_values: Values from target table

        Returns:
            Distribution similarity score (0.0 to 1.0)
        """
        if not source_values or not target_values:
            return 0.0

        # Calculate value frequency distributions
        source_freq = self._calculate_frequency_distribution(source_values)
        target_freq = self._calculate_frequency_distribution(target_values)

        # Get common values
        common_values = set(source_freq.keys()).intersection(set(target_freq.keys()))
        
        if not common_values:
            return 0.0

        # Calculate similarity for common values
        total_similarity = 0.0
        for value in common_values:
            source_ratio = source_freq[value] / len(source_values)
            target_ratio = target_freq[value] / len(target_values)
            
            # Similarity is 1 - absolute difference in ratios
            similarity = 1.0 - abs(source_ratio - target_ratio)
            total_similarity += similarity

        # Average similarity weighted by common value coverage
        common_coverage = len(common_values) / max(len(source_freq), len(target_freq))
        avg_similarity = total_similarity / len(common_values) if common_values else 0.0
        
        # Weight by coverage
        final_similarity = avg_similarity * common_coverage

        logger.debug(f"Distribution similarity: {final_similarity:.3f} (coverage: {common_coverage:.3f})")
        return final_similarity

    def _calculate_frequency_distribution(self, values: List[Any]) -> Dict[Any, int]:
        """Calculate frequency distribution of values.

        Args:
            values: List of values

        Returns:
            Dictionary mapping values to their frequencies
        """
        freq = {}
        for value in values:
            freq[value] = freq.get(value, 0) + 1
        return freq

    def _calculate_overall_confidence(self, referential_integrity: float, 
                                    type_compatibility: float, 
                                    distribution_similarity: float) -> float:
        """Calculate overall confidence score.

        Args:
            referential_integrity: Referential integrity score
            type_compatibility: Type compatibility score
            distribution_similarity: Distribution similarity score

        Returns:
            Overall confidence score (0.0 to 1.0)
        """
        # Weighted average with referential integrity being most important
        weights = {
            'referential_integrity': 0.5,
            'type_compatibility': 0.3,
            'distribution_similarity': 0.2
        }

        overall = (
            referential_integrity * weights['referential_integrity'] +
            type_compatibility * weights['type_compatibility'] +
            distribution_similarity * weights['distribution_similarity']
        )

        return min(1.0, max(0.0, overall))

    def get_adaptive_sample_size(self, table_name: str, target_confidence: float = 0.95) -> int:
        """Get adaptive sample size based on table characteristics.

        Args:
            table_name: Name of the table
            target_confidence: Target confidence level

        Returns:
            Recommended sample size
        """
        try:
            # Get table row count
            if self.connector:
                query = f"SELECT COUNT(*) as row_count FROM `{table_name}`"
                result = self.connector.client.query(query).result()
                row_count = next(result)[0]
            else:
                # Fallback to BQ CLI
                query = f"SELECT COUNT(*) as row_count FROM `{table_name}`"
                result = self.bq_cli_connector._execute_bq_query(query)
                row_count = result[0][0] if result else 0

            if row_count < 1000:
                return row_count

            # Calculate required sample size for target confidence
            return self._calculate_sample_size(row_count, target_confidence)

        except Exception as e:
            logger.error(f"Error getting table size for {table_name}: {e}")
            return 1000  # Default fallback

    def _calculate_sample_size(self, population_size: int, confidence_level: float = 0.95) -> int:
        """Calculate required sample size for statistical confidence.

        Args:
            population_size: Total population size
            confidence_level: Desired confidence level

        Returns:
            Required sample size
        """
        # Z-score for confidence level
        z_scores = {0.90: 1.645, 0.95: 1.96, 0.99: 2.576}
        z = z_scores.get(confidence_level, 1.96)

        # Margin of error (5%)
        margin_of_error = 0.05

        # Calculate sample size using Cochran's formula
        n = (z**2 * 0.25) / (margin_of_error**2)

        # Adjust for finite population
        if population_size < n:
            return population_size

        # Apply finite population correction
        n_adjusted = n / (1 + (n - 1) / population_size)

        return min(int(n_adjusted), population_size)
