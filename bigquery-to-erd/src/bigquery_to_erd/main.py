"""Main CLI interface for BigQuery to ERD tool."""

import logging
import sys
from pathlib import Path
from typing import Optional

import click
from google.cloud.exceptions import GoogleCloudError

from .config import Config
from .models import ERDConfig, OutputFormat, TableLayout
from .bigquery_connector import BigQueryConnector
from .bq_cli_connector import BQCLIConnector
from .schema_analyzer import SchemaAnalyzer
from .relationship_detector import RelationshipDetector, RelationshipValidator
from .enhanced_relationship_detector import EnhancedRelationshipDetector
from .erd_generator import ERDGenerator


# Configure logging
def setup_logging(log_level: str, log_file: Optional[str] = None):
    """Setup logging configuration.
    
    Args:
        log_level: Logging level
        log_file: Optional log file path
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )


@click.command()
@click.option('--dataset-id', help='BigQuery dataset ID (overrides .env)')
@click.option('--project-id', help='GCP project ID (overrides .env)')
@click.option('--output-file', help='Output file path')
@click.option('--format', 'output_format', 
              type=click.Choice(['drawio', 'mermaid', 'plantuml']),
              help='Output format')
@click.option('--include-views', is_flag=True, help='Include views in ERD')
@click.option('--include-external-tables', is_flag=True, help='Include external tables')
@click.option('--custom-rules', help='Path to custom relationship rules JSON file')
@click.option('--table-layout', 
              type=click.Choice(['auto', 'horizontal', 'vertical', 'force_directed', 'hierarchical', 'grid']),
              help='Table layout algorithm')
@click.option('--show-column-types/--no-show-column-types', default=True, 
              help='Show column data types')
@click.option('--show-column-nullable/--no-show-column-nullable', default=True,
              help='Show nullable indicators')
@click.option('--show-indexes/--no-show-indexes', default=False,
              help='Show indexes')
@click.option('--drawio-theme', 
              type=click.Choice(['default', 'dark', 'minimal']),
              help='Draw.io theme')
@click.option('--enable-data-testing/--no-data-testing', default=True,
              help='Enable data-based relationship testing')
@click.option('--enable-parallel/--no-parallel', default=True,
              help='Enable parallel processing')
@click.option('--enable-caching/--no-caching', default=True,
              help='Enable relationship caching')
@click.option('--enable-incremental/--no-incremental', default=True,
              help='Enable incremental processing')
@click.option('--pattern-config', help='Path to pattern configuration file')
@click.option('--cache-dir', default='.cache', help='Directory for caching relationships')
@click.option('--state-file', default='relationship_state.json', help='Path to state persistence file')
@click.option('--env-file', help='Path to .env file')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.option('--dry-run', is_flag=True, help='Show what would be done without executing')
def main(dataset_id: Optional[str],
         project_id: Optional[str],
         output_file: Optional[str],
         output_format: Optional[str],
         include_views: Optional[bool],
         include_external_tables: Optional[bool],
         custom_rules: Optional[str],
         table_layout: Optional[str],
         show_column_types: Optional[bool],
         show_column_nullable: Optional[bool],
         show_indexes: Optional[bool],
         drawio_theme: Optional[str],
         enable_data_testing: bool,
         enable_parallel: bool,
         enable_caching: bool,
         enable_incremental: bool,
         pattern_config: Optional[str],
         cache_dir: str,
         state_file: str,
         env_file: Optional[str],
         verbose: bool,
         dry_run: bool):
    """Generate Entity Relationship Diagrams from BigQuery datasets.
    
    This tool connects to BigQuery, extracts schema information from a dataset,
    detects relationships between tables, and generates an ERD in various formats.
    
    Examples:
    
        # Basic usage with .env file
        bigquery-to-erd
        
        # Override specific settings
        bigquery-to-erd --dataset-id my_dataset --output-file my_erd.drawio
        
        # Generate Mermaid format
        bigquery-to-erd --format mermaid --output-file schema.mmd
        
        # Include views and external tables
        bigquery-to-erd --include-views --include-external-tables
    """
    try:
        # Load configuration
        config_manager = Config(env_file)
        
        # Build configuration overrides
        overrides = {}
        if dataset_id:
            overrides['dataset_id'] = dataset_id
        if project_id:
            overrides['project_id'] = project_id
        if output_file:
            overrides['output_file'] = output_file
        if output_format:
            overrides['output_format'] = output_format
        if include_views is not None:
            overrides['include_views'] = include_views
        if include_external_tables is not None:
            overrides['include_external_tables'] = include_external_tables
        if custom_rules:
            overrides['custom_relationship_rules_file'] = custom_rules
        if table_layout:
            overrides['table_layout'] = table_layout
        if show_column_types is not None:
            overrides['show_column_types'] = show_column_types
        if show_column_nullable is not None:
            overrides['show_column_nullable'] = show_column_nullable
        if show_indexes is not None:
            overrides['show_indexes'] = show_indexes
        if drawio_theme:
            overrides['drawio_theme'] = drawio_theme
        
        # Get configuration
        config = config_manager.get_erd_config(**overrides)
        
        # Setup logging
        log_level = 'DEBUG' if verbose else config.log_level
        setup_logging(log_level, config.log_file)
        
        logger = logging.getLogger(__name__)
        logger.info(f"Starting BigQuery to ERD generation for dataset: {config.dataset_id}")
        
        if dry_run:
            click.echo("DRY RUN - Configuration:")
            click.echo(f"  Project ID: {config.project_id}")
            click.echo(f"  Dataset ID: {config.dataset_id}")
            click.echo(f"  Output Format: {config.output_format}")
            click.echo(f"  Output File: {config.output_file}")
            click.echo(f"  Include Views: {config.include_views}")
            click.echo(f"  Include External Tables: {config.include_external_tables}")
            click.echo(f"  Table Layout: {config.table_layout}")
            click.echo(f"  Data Testing: {enable_data_testing}")
            click.echo(f"  Parallel Processing: {enable_parallel}")
            click.echo(f"  Caching: {enable_caching}")
            click.echo(f"  Incremental Processing: {enable_incremental}")
            click.echo(f"  Pattern Config: {pattern_config or 'default'}")
            click.echo(f"  Cache Directory: {cache_dir}")
            click.echo(f"  State File: {state_file}")
            return
        
        # Validate configuration
        config_manager.validate_config(config)
        
        # Load custom rules if specified
        custom_rules_config = None
        if config.custom_relationship_rules_file:
            custom_rules_config = config_manager.get_custom_rules_config()
        
        # Initialize components
        credentials_path = config_manager.get_google_credentials_path()
        analyzer = SchemaAnalyzer()
        
        # Use enhanced relationship detector if enabled
        if enable_data_testing or enable_parallel or enable_caching or enable_incremental:
            detector = EnhancedRelationshipDetector(
                pattern_config_file=pattern_config,
                cache_dir=cache_dir,
                state_file=state_file
            )
            validator = RelationshipValidator()
        else:
            detector = RelationshipDetector(custom_rules_config)
            validator = RelationshipValidator()
        
        generator = ERDGenerator(config)
        
        # Try Python client first, fallback to BQ CLI
        connector = None
        tables = []
        
        try:
            # Try Python BigQuery client first
            click.echo("Connecting to BigQuery using Python client...")
            connector = BigQueryConnector(config, credentials_path)
            connector.connect()
            
            if connector.test_connection():
                click.echo(f"Extracting schemas from dataset: {config.dataset_id}")
                tables = connector.get_all_table_schemas()
            else:
                raise RuntimeError("Python client connection test failed")
                
        except Exception as e:
            logger.warning(f"Python client failed: {e}")
            click.echo("Python client failed, trying BQ CLI...")
            
            # Fallback to BQ CLI
            bq_connector = BQCLIConnector(config)
            if not bq_connector.test_connection():
                raise RuntimeError("Neither Python client nor BQ CLI is available")
            
            click.echo(f"Extracting schemas from dataset: {config.dataset_id} using BQ CLI")
            tables = bq_connector.get_all_table_schemas()
        
        if not tables:
            click.echo("No tables found in dataset")
            return
        
        click.echo(f"Found {len(tables)} tables")
        
        # Analyze schemas
        click.echo("Analyzing table schemas...")
        analyzed_tables = []
        for table in tables:
            analyzed_table = analyzer.parse_table_schema(table)
            analyzed_tables.append(analyzed_table)
        
        # Detect relationships
        click.echo("Detecting relationships...")
        if hasattr(detector, 'detect_relationships_enhanced'):
            # Use enhanced detector
            relationships = detector.detect_relationships_enhanced(
                analyzed_tables,
                enable_data_testing=enable_data_testing,
                enable_parallel=enable_parallel
            )
            valid_relationships = relationships
        else:
            # Use standard detector
            relationships = detector.detect_relationships(
                analyzed_tables,
                enable_fk_detection=config.enable_fk_detection,
                enable_naming_convention_detection=config.enable_naming_convention_detection
            )
            valid_relationships = validator.validate_relationships(relationships, analyzed_tables)
        
        click.echo(f"Detected {len(valid_relationships)} valid relationships")
        
        # Generate ERD
        click.echo(f"Generating ERD in {config.output_format} format...")
        erd_content = generator.generate_erd(analyzed_tables, valid_relationships)
        
        # Write output file
        output_path = Path(config.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(erd_content)
        
        click.echo(f"ERD generated successfully: {output_path}")
        click.echo(f"  Tables: {len(analyzed_tables)}")
        click.echo(f"  Relationships: {len(valid_relationships)}")
        click.echo(f"  Format: {config.output_format}")
        
        # Show enhanced features stats if using enhanced detector
        if hasattr(detector, 'get_processing_stats'):
            stats = detector.get_processing_stats()
            click.echo(f"  Processing Stats:")
            click.echo(f"    Parallel Processing: {stats.get('parallel_processing', {}).get('parallel_enabled', False)}")
            click.echo(f"    Data Testing: {stats.get('data_testing_enabled', False)}")
            click.echo(f"    Caching: {stats.get('cache_enabled', False)}")
            click.echo(f"    Incremental Processing: {stats.get('incremental_processing', False)}")
            
            if 'cache_stats' in stats:
                cache_stats = stats['cache_stats']
                click.echo(f"    Cache Entries: {cache_stats.get('memory_cache_entries', 0)}")
            
            if 'incremental_stats' in stats:
                inc_stats = stats['incremental_stats']
                click.echo(f"    Processed Tables: {inc_stats.get('processed_tables', 0)}")
        
        # Show relationship quality report if using enhanced detector
        if hasattr(detector, 'get_relationship_quality_report'):
            quality_report = detector.get_relationship_quality_report(valid_relationships)
            click.echo(f"  Relationship Quality:")
            click.echo(f"    High Confidence: {quality_report.get('confidence_distribution', {}).get('high_confidence', 0)}")
            click.echo(f"    Medium Confidence: {quality_report.get('confidence_distribution', {}).get('medium_confidence', 0)}")
            click.echo(f"    Low Confidence: {quality_report.get('confidence_distribution', {}).get('low_confidence', 0)}")
            click.echo(f"    Average Confidence: {quality_report.get('average_confidence', 0):.3f}")
        
        # Close connection if using Python client
        if connector:
            connector.close()
        
    except KeyboardInterrupt:
        click.echo("\nOperation cancelled by user")
        sys.exit(1)
    except GoogleCloudError as e:
        click.echo(f"BigQuery error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
