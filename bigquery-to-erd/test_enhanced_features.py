#!/usr/bin/env python3
"""Test script for enhanced relationship detection features."""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from bigquery_to_erd.enhanced_relationship_detector import EnhancedRelationshipDetector
from bigquery_to_erd.data_relationship_tester import DataRelationshipTester
from bigquery_to_erd.relationship_cache import RelationshipCache
from bigquery_to_erd.incremental_processor import IncrementalProcessor
from bigquery_to_erd.parallel_processor import ParallelProcessor, ProcessingConfig
from bigquery_to_erd.pattern_config import PatternConfigLoader
from bigquery_to_erd.models import TableSchema, ColumnInfo, Relationship

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_pattern_config():
    """Test pattern configuration loading."""
    print("\n=== Testing Pattern Configuration ===")
    
    try:
        config_loader = PatternConfigLoader()
        
        # Test data testing config
        data_config = config_loader.get_data_testing_config()
        print(f"Data Testing Config:")
        print(f"  Enabled: {data_config.enabled}")
        print(f"  Sample Size: {data_config.sample_size}")
        print(f"  Confidence Threshold: {data_config.confidence_threshold}")
        print(f"  Adaptive Sampling: {data_config.adaptive_sampling}")
        
        # Test performance config
        perf_config = config_loader.get_performance_config()
        print(f"\nPerformance Config:")
        print(f"  Parallel Processing: {perf_config.parallel_processing}")
        print(f"  Max Workers: {perf_config.max_workers}")
        print(f"  Cache Enabled: {perf_config.cache_enabled}")
        print(f"  Incremental Processing: {perf_config.incremental_processing}")
        
        # Test table pattern matching
        test_tables = ["h_customer", "dim_product", "l_order_item", "ref_status"]
        print(f"\nTable Pattern Matching:")
        for table in test_tables:
            patterns = config_loader.get_patterns_for_table(table)
            print(f"  {table}: {[p[1] for p in patterns]}")
            
            # Test PK/FK detection
            pk_candidates = ["id", "key", "hash_key", "customer_id", "product_sk"]
            fk_candidates = ["customer_id", "product_hk", "order_id", "status_code"]
            
            print(f"    PK candidates: {[col for col in pk_candidates if config_loader.is_primary_key_candidate(col, table)]}")
            print(f"    FK candidates: {[col for col in fk_candidates if config_loader.is_foreign_key_candidate(col, table)]}")
        
        print("✅ Pattern configuration test passed")
        return True
        
    except Exception as e:
        print(f"❌ Pattern configuration test failed: {e}")
        return False


def test_relationship_cache():
    """Test relationship caching."""
    print("\n=== Testing Relationship Cache ===")
    
    try:
        cache = RelationshipCache(".test_cache")
        
        # Test caching
        test_relationship = Relationship(
            source_table="h_customer",
            target_table="dim_customer",
            source_column="id",
            target_column="customer_id",
            relationship_type="one_to_many",
            confidence=0.9,
            detection_method="enhanced_pk_fk"
        )
        
        # Cache relationship
        cache.cache_relationship(test_relationship)
        print(f"Cached relationship: {test_relationship.source_table} -> {test_relationship.target_table}")
        
        # Retrieve from cache
        cached = cache.get_cached_relationship("h_customer", "dim_customer")
        if cached:
            print(f"Retrieved from cache: {cached.source_table} -> {cached.target_table}")
            print(f"  Confidence: {cached.confidence}")
            print(f"  Method: {cached.detection_method}")
        else:
            print("❌ Failed to retrieve from cache")
            return False
        
        # Test cache stats
        stats = cache.get_cache_stats()
        print(f"Cache stats: {stats}")
        
        # Clean up
        cache.clear_cache()
        print("✅ Relationship cache test passed")
        return True
        
    except Exception as e:
        print(f"❌ Relationship cache test failed: {e}")
        return False


def test_incremental_processor():
    """Test incremental processing."""
    print("\n=== Testing Incremental Processor ===")
    
    try:
        processor = IncrementalProcessor(".test_state.json")
        
        # Create test tables
        test_tables = [
            TableSchema(
                table_id="h_customer",
                project_id="test_project",
                dataset_id="test_dataset",
                columns=[
                    ColumnInfo(name="id", data_type="STRING", mode="REQUIRED", is_primary_key=True),
                    ColumnInfo(name="business_key", data_type="STRING", mode="REQUIRED")
                ]
            ),
            TableSchema(
                table_id="dim_customer",
                project_id="test_project", 
                dataset_id="test_dataset",
                columns=[
                    ColumnInfo(name="customer_id", data_type="STRING", mode="REQUIRED", is_primary_key=True),
                    ColumnInfo(name="name", data_type="STRING", mode="NULLABLE")
                ]
            )
        ]
        
        # Test table change detection
        for table in test_tables:
            is_changed = processor.is_table_changed(table)
            print(f"Table {table.table_id} changed: {is_changed}")
        
        # Test getting tables to process
        tables_to_process = processor.get_tables_to_process(test_tables)
        print(f"Tables to process: {[t.table_id for t in tables_to_process]}")
        
        # Test marking as processed
        for table in test_tables:
            processor.mark_table_processed(table)
        
        # Test state persistence
        processor.save_state()
        print("State saved")
        
        # Test loading state
        new_processor = IncrementalProcessor(".test_state.json")
        print(f"Loaded state: {len(new_processor.processed_tables)} processed tables")
        
        # Clean up
        processor.clear_state()
        print("✅ Incremental processor test passed")
        return True
        
    except Exception as e:
        print(f"❌ Incremental processor test failed: {e}")
        return False


def test_parallel_processor():
    """Test parallel processing."""
    print("\n=== Testing Parallel Processor ===")
    
    try:
        config = ProcessingConfig(max_workers=2, batch_size=2, enable_parallel=True)
        processor = ParallelProcessor(config)
        
        # Test processing stats
        stats = processor.get_processing_stats()
        print(f"Processing stats: {stats}")
        
        print("✅ Parallel processor test passed")
        return True
        
    except Exception as e:
        print(f"❌ Parallel processor test failed: {e}")
        return False


def test_enhanced_detector():
    """Test enhanced relationship detector."""
    print("\n=== Testing Enhanced Relationship Detector ===")
    
    try:
        detector = EnhancedRelationshipDetector(
            pattern_config_file=None,  # Use default
            cache_dir=".test_cache",
            state_file=".test_state.json"
        )
        
        # Test processing stats
        stats = detector.get_processing_stats()
        print(f"Enhanced detector stats: {stats}")
        
        # Test quality report with sample relationships
        sample_relationships = [
            Relationship(
                source_table="h_customer",
                target_table="dim_customer", 
                source_column="id",
                target_column="customer_id",
                relationship_type="one_to_many",
                confidence=0.9,
                detection_method="enhanced_pk_fk"
            ),
            Relationship(
                source_table="dim_customer",
                target_table="l_order",
                source_column="customer_id", 
                target_column="customer_hk",
                relationship_type="one_to_many",
                confidence=0.7,
                detection_method="data_vault_pattern"
            )
        ]
        
        quality_report = detector.get_relationship_quality_report(sample_relationships)
        print(f"Quality report: {quality_report}")
        
        # Clean up
        detector.clear_cache()
        print("✅ Enhanced relationship detector test passed")
        return True
        
    except Exception as e:
        print(f"❌ Enhanced relationship detector test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🚀 Testing Enhanced Relationship Detection Features")
    
    tests = [
        test_pattern_config,
        test_relationship_cache,
        test_incremental_processor,
        test_parallel_processor,
        test_enhanced_detector
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
