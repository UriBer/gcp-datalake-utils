# Enhanced Relationship Detection Features

This document describes the advanced features implemented in the BigQuery to ERD tool for data-based relationship testing and performance optimization.

## Overview

The enhanced relationship detection system provides:

1. **Data-Based Testing**: Validates relationships using actual data samples
2. **Relationship Caching**: Speeds up processing by caching detected relationships
3. **Incremental Processing**: Only processes changed tables to avoid reprocessing
4. **Parallel Processing**: Uses multiple workers for faster relationship detection
5. **Configuration-Driven**: All patterns and rules are externalized to JSON configuration

## Features

### 1. Data-Based Relationship Testing

The system can validate relationships by analyzing actual data samples from BigQuery tables.

**Key Components:**
- `DataRelationshipTester`: Tests relationships using statistical analysis
- `DataTestResult`: Contains test results with confidence scores
- Adaptive sampling based on table size and confidence requirements

**Features:**
- **Referential Integrity**: Checks if source values exist in target tables
- **Type Compatibility**: Validates data type compatibility between columns
- **Distribution Analysis**: Compares value distributions between related columns
- **Adaptive Sampling**: Automatically determines optimal sample size

**Configuration:**
```json
{
  "data_testing": {
    "enabled": true,
    "sample_size": 1000,
    "confidence_threshold": 0.8,
    "max_orphan_percentage": 0.1,
    "type_compatibility_required": true,
    "distribution_similarity_threshold": 0.7,
    "adaptive_sampling": true,
    "target_confidence_level": 0.95
  }
}
```

### 2. Relationship Caching

Caches detected relationships to avoid reprocessing and improve performance.

**Key Components:**
- `RelationshipCache`: Manages memory and disk-based caching
- TTL-based cache invalidation
- Pattern-based cache clearing

**Features:**
- **Memory Cache**: Fast access to recently used relationships
- **Disk Persistence**: Survives application restarts
- **TTL Support**: Automatic cache expiration
- **Pattern Clearing**: Clear cache for specific table patterns

**Usage:**
```python
cache = RelationshipCache(".cache")
cache.cache_relationship(relationship)
cached = cache.get_cached_relationship("table1", "table2")
```

### 3. Incremental Processing

Only processes tables that have changed since the last run, significantly improving performance for large datasets.

**Key Components:**
- `IncrementalProcessor`: Manages processing state
- Table checksum calculation for change detection
- State persistence across runs

**Features:**
- **Change Detection**: Calculates checksums to detect table changes
- **State Persistence**: Saves processing state to disk
- **Selective Processing**: Only processes changed tables
- **State Management**: Tracks processed tables and relationships

**Usage:**
```python
processor = IncrementalProcessor("state.json")
tables_to_process = processor.get_tables_to_process(all_tables)
processor.mark_table_processed(table)
```

### 4. Parallel Processing

Uses multiple workers to process relationship detection in parallel.

**Key Components:**
- `ParallelProcessor`: Manages parallel execution
- `ProcessingConfig`: Configures parallel processing parameters
- Thread-based parallel execution

**Features:**
- **Configurable Workers**: Set number of parallel workers
- **Batch Processing**: Groups tables for efficient processing
- **Timeout Handling**: Prevents hanging on problematic tables
- **Type-Based Grouping**: Groups tables by type for better batching

**Configuration:**
```json
{
  "performance": {
    "parallel_processing": true,
    "max_workers": 4,
    "batch_size": 10,
    "timeout_seconds": 300,
    "group_tables_by_type": true
  }
}
```

### 5. Enhanced Relationship Detector

Integrates all enhanced features into a single, easy-to-use interface.

**Key Components:**
- `EnhancedRelationshipDetector`: Main interface for enhanced detection
- Integrates caching, incremental processing, and parallel execution
- Quality reporting and statistics

**Features:**
- **Unified Interface**: Single class for all enhanced features
- **Quality Reporting**: Detailed relationship quality metrics
- **Processing Statistics**: Performance and usage statistics
- **Configurable**: All features can be enabled/disabled

**Usage:**
```python
detector = EnhancedRelationshipDetector(
    pattern_config_file="config/relationship_patterns.json",
    cache_dir=".cache",
    state_file="state.json"
)

relationships = detector.detect_relationships_enhanced(
    tables,
    enable_data_testing=True,
    enable_parallel=True
)
```

## Configuration

### Pattern Configuration

The system uses a comprehensive JSON configuration file (`config/relationship_patterns.json`) that defines:

1. **Table Patterns**: Data Vault and traditional DW patterns
2. **Primary Key Patterns**: Rules for identifying primary keys
3. **Foreign Key Patterns**: Rules for identifying foreign keys
4. **Data Testing Settings**: Configuration for data-based testing
5. **Performance Settings**: Parallel processing and caching configuration

### CLI Integration

The enhanced features are integrated into the main CLI with new options:

```bash
# Enable all enhanced features
python -m bigquery_to_erd.main --enable-data-testing --enable-parallel --enable-caching --enable-incremental

# Use custom configuration
python -m bigquery_to_erd.main --pattern-config custom_patterns.json --cache-dir .my_cache

# Disable specific features
python -m bigquery_to_erd.main --no-data-testing --no-parallel
```

**New CLI Options:**
- `--enable-data-testing/--no-data-testing`: Enable data-based testing
- `--enable-parallel/--no-parallel`: Enable parallel processing
- `--enable-caching/--no-caching`: Enable relationship caching
- `--enable-incremental/--no-incremental`: Enable incremental processing
- `--pattern-config`: Path to pattern configuration file
- `--cache-dir`: Directory for caching relationships
- `--state-file`: Path to state persistence file

## Performance Benefits

### Caching
- **First Run**: Normal processing time
- **Subsequent Runs**: 50-90% faster with cache hits
- **Memory Usage**: Minimal overhead with TTL-based cleanup

### Incremental Processing
- **Large Datasets**: Only processes changed tables
- **Development**: Fast iteration during schema changes
- **Production**: Efficient updates without full reprocessing

### Parallel Processing
- **Multi-core Systems**: Utilizes all available CPU cores
- **Large Datasets**: 2-4x faster processing on multi-core systems
- **Scalability**: Performance scales with available cores

### Data Testing
- **Accuracy**: Higher confidence in detected relationships
- **Validation**: Catches false positives from naming conventions
- **Quality**: Better relationship quality scores

## Usage Examples

### Basic Enhanced Usage

```python
from bigquery_to_erd.enhanced_relationship_detector import EnhancedRelationshipDetector

# Initialize with default configuration
detector = EnhancedRelationshipDetector()

# Detect relationships with all enhanced features
relationships = detector.detect_relationships_enhanced(
    tables,
    enable_data_testing=True,
    enable_parallel=True
)

# Get quality report
quality_report = detector.get_relationship_quality_report(relationships)
print(f"High confidence relationships: {quality_report['confidence_distribution']['high_confidence']}")
```

### Custom Configuration

```python
# Use custom pattern configuration
detector = EnhancedRelationshipDetector(
    pattern_config_file="custom_patterns.json",
    cache_dir=".my_cache",
    state_file="my_state.json"
)

# Get processing statistics
stats = detector.get_processing_stats()
print(f"Cache entries: {stats['cache_stats']['memory_cache_entries']}")
```

### CLI Usage

```bash
# Full enhanced processing
python -m bigquery_to_erd.main \
  --dataset-id my_dataset \
  --enable-data-testing \
  --enable-parallel \
  --enable-caching \
  --enable-incremental \
  --pattern-config custom_patterns.json \
  --cache-dir .cache \
  --state-file state.json

# Quick processing without data testing
python -m bigquery_to_erd.main \
  --dataset-id my_dataset \
  --no-data-testing \
  --enable-parallel \
  --enable-caching
```

## Monitoring and Debugging

### Quality Metrics

The system provides detailed quality metrics:

- **Confidence Distribution**: High/medium/low confidence relationships
- **Detection Methods**: Which methods detected each relationship
- **Relationship Types**: Distribution of relationship types
- **Average Confidence**: Overall relationship quality score

### Processing Statistics

- **Cache Performance**: Hit rates and entry counts
- **Incremental Processing**: Tables processed vs. total
- **Parallel Processing**: Worker utilization and performance
- **Data Testing**: Sample sizes and validation results

### Logging

Comprehensive logging at multiple levels:

- **DEBUG**: Detailed processing information
- **INFO**: Progress updates and statistics
- **WARNING**: Non-critical issues
- **ERROR**: Processing errors and failures

## Best Practices

### Configuration
1. **Start with defaults**: Use default configuration initially
2. **Tune for your data**: Adjust patterns based on your naming conventions
3. **Monitor performance**: Use statistics to optimize settings
4. **Version control**: Keep configuration files in version control

### Performance
1. **Enable caching**: Always enable caching for production use
2. **Use incremental**: Enable incremental processing for large datasets
3. **Parallel processing**: Enable parallel processing on multi-core systems
4. **Data testing**: Use data testing for critical relationships

### Maintenance
1. **Clear cache**: Periodically clear cache for stale data
2. **Monitor state**: Check incremental processing state files
3. **Update patterns**: Update patterns as schemas evolve
4. **Review quality**: Regularly review relationship quality reports

## Troubleshooting

### Common Issues

1. **Cache not working**: Check cache directory permissions
2. **Incremental processing stuck**: Clear state file to reset
3. **Parallel processing slow**: Reduce max_workers or increase batch_size
4. **Data testing errors**: Check BigQuery permissions and table access

### Debug Mode

Enable debug logging for detailed information:

```bash
python -m bigquery_to_erd.main --verbose --dataset-id my_dataset
```

### Reset State

To reset all state and start fresh:

```python
detector = EnhancedRelationshipDetector()
detector.clear_cache()  # Clear all cached relationships
# State will be reset on next run
```

## Future Enhancements

Planned improvements include:

1. **Machine Learning**: ML-based relationship detection
2. **Real-time Updates**: Live relationship monitoring
3. **Advanced Caching**: Distributed caching for multiple instances
4. **Visual Analytics**: Relationship quality visualization
5. **API Integration**: REST API for relationship detection
