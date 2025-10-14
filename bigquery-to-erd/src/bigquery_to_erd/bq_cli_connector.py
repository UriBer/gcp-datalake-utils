"""BigQuery CLI connector as a fallback when Python client lacks permissions."""

import logging
import subprocess
import json
import tempfile
from typing import List, Optional, Dict, Any
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from .models import TableSchema, ColumnInfo, ERDConfig


logger = logging.getLogger(__name__)


class BQCLIConnector:
    """BigQuery CLI connector for when Python client lacks permissions."""
    
    def __init__(self, config: ERDConfig):
        """Initialize BQ CLI connector.
        
        Args:
            config: ERD configuration
        """
        self.config = config
        self.project_id = config.project_id
        self.dataset_id = config.dataset_id
        self.location = config.location
    
    def test_connection(self) -> bool:
        """Test if bq CLI is available and working.
        
        Returns:
            True if bq CLI is working
        """
        try:
            result = subprocess.run(
                ['bq', 'help'],
                capture_output=True,
                text=True,
                timeout=10
            )
            # bq help returns exit code 1 but still works
            return "BigQuery" in result.stdout or "bq" in result.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False
    
    def list_tables(self) -> List[str]:
        """List all tables in the dataset using bq CLI.
        
        Returns:
            List of table IDs
            
        Raises:
            RuntimeError: If bq command fails
        """
        try:
            cmd = [
                'bq', 'ls', 
                '--format=json',
                '--max_results', str(self.config.max_results),
                f'{self.project_id}:{self.dataset_id}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                raise RuntimeError(f"bq ls failed: {result.stderr}")
            
            # Parse JSON output
            tables_data = json.loads(result.stdout)
            table_ids = [table['tableReference']['tableId'] for table in tables_data]
            
            logger.info(f"Found {len(table_ids)} tables using bq CLI")
            return table_ids
            
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse bq output: {e}")
        except Exception as e:
            raise RuntimeError(f"bq ls command failed: {e}")
    
    def get_table_schema(self, table_id: str) -> TableSchema:
        """Get schema information for a table using bq CLI.
        
        Args:
            table_id: Table ID
            
        Returns:
            TableSchema object
            
        Raises:
            RuntimeError: If bq command fails
        """
        try:
            # Get table schema using bq show
            cmd = [
                'bq', 'show',
                '--format=json',
                f'{self.project_id}:{self.dataset_id}.{table_id}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            
            if result.returncode != 0:
                raise RuntimeError(f"bq show failed for {table_id}: {result.stderr}")
            
            # Parse JSON output
            table_data = json.loads(result.stdout)
            
            # Extract column information
            columns = []
            for field in table_data.get('schema', {}).get('fields', []):
                column_info = ColumnInfo(
                    name=field['name'],
                    data_type=field['type'],
                    mode=field.get('mode', 'NULLABLE'),
                    description=field.get('description'),
                    max_length=field.get('maxLength'),
                    precision=field.get('precision'),
                    scale=field.get('scale')
                )
                columns.append(column_info)
            
            # Create TableSchema
            table_schema = TableSchema(
                table_id=table_data['tableReference']['tableId'],
                dataset_id=table_data['tableReference']['datasetId'],
                project_id=table_data['tableReference']['projectId'],
                description=table_data.get('description'),
                columns=columns,
                num_rows=table_data.get('numRows'),
                num_bytes=table_data.get('numBytes'),
                created=table_data.get('creationTime'),
                modified=table_data.get('lastModifiedTime'),
                table_type=table_data.get('type', 'TABLE'),
                labels=table_data.get('labels', {})
            )
            
            logger.debug(f"Extracted schema for table {table_id}: {len(columns)} columns")
            return table_schema
            
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse bq show output for {table_id}: {e}")
        except Exception as e:
            raise RuntimeError(f"bq show command failed for {table_id}: {e}")
    
    def get_all_table_schemas(self) -> List[TableSchema]:
        """Get schemas for all tables in the dataset using parallel processing.
        
        Returns:
            List of TableSchema objects
        """
        table_ids = self.list_tables()
        logger.info(f"Found {len(table_ids)} tables, extracting schemas in parallel...")
        
        # Get table types in one batch call
        table_types = self._get_table_types_batch(table_ids)
        
        # Filter tables based on type
        tables_to_process = []
        for table_id in table_ids:
            table_type = table_types.get(table_id, 'TABLE')
            
            should_include = True
            if table_type == "VIEW" and not self.config.include_views:
                should_include = False
            elif table_type == "EXTERNAL" and not self.config.include_external_tables:
                should_include = False
            
            if should_include:
                tables_to_process.append(table_id)
            else:
                logger.debug(f"Skipping {table_type} table: {table_id}")
        
        logger.info(f"Processing {len(tables_to_process)} tables (filtered from {len(table_ids)})")
        
        # Process tables in parallel
        schemas = []
        max_workers = min(8, len(tables_to_process))  # Limit to 8 workers to avoid overwhelming BigQuery
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_table = {
                executor.submit(self._get_table_schema_safe, table_id): table_id 
                for table_id in tables_to_process
            }
            
            # Collect results as they complete
            completed = 0
            start_time = time.time()
            for future in as_completed(future_to_table):
                table_id = future_to_table[future]
                try:
                    schema = future.result(timeout=30)  # 30 second timeout per table
                    if schema:
                        schemas.append(schema)
                    completed += 1
                    
                    # Progress reporting
                    if completed % 5 == 0 or completed == len(tables_to_process):
                        elapsed = time.time() - start_time
                        rate = completed / elapsed if elapsed > 0 else 0
                        eta = (len(tables_to_process) - completed) / rate if rate > 0 else 0
                        logger.info(f"Progress: {completed}/{len(tables_to_process)} tables "
                                  f"({completed/len(tables_to_process)*100:.1f}%) - "
                                  f"Rate: {rate:.1f} tables/sec - ETA: {eta:.0f}s")
                        
                except Exception as e:
                    logger.warning(f"Failed to get schema for table {table_id}: {e}")
                    completed += 1
        
        logger.info(f"Successfully extracted schemas for {len(schemas)} tables using bq CLI")
        return schemas
    
    def _get_table_types_batch(self, table_ids: List[str]) -> Dict[str, str]:
        """Get table types for all tables in one batch call.
        
        Args:
            table_ids: List of table IDs
            
        Returns:
            Dictionary mapping table_id to table_type
        """
        try:
            cmd = ['bq', 'ls', '--format=json', f'{self.project_id}:{self.dataset_id}']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                tables_data = json.loads(result.stdout)
                table_types = {}
                for table_info in tables_data:
                    table_id = table_info['tableReference']['tableId']
                    table_type = table_info.get('type', 'TABLE')
                    table_types[table_id] = table_type
                return table_types
            else:
                logger.warning(f"Failed to get table types: {result.stderr}")
                return {table_id: 'TABLE' for table_id in table_ids}
        except Exception as e:
            logger.warning(f"Error getting table types: {e}")
            return {table_id: 'TABLE' for table_id in table_ids}
    
    def _get_table_schema_safe(self, table_id: str) -> Optional[TableSchema]:
        """Safely get table schema with error handling.
        
        Args:
            table_id: Table ID
            
        Returns:
            TableSchema or None if failed
        """
        try:
            return self.get_table_schema(table_id)
        except Exception as e:
            logger.debug(f"Failed to get schema for {table_id}: {e}")
            return None
    
    def get_table_metadata(self, table_id: str) -> Dict[str, Any]:
        """Get metadata for a table.
        
        Args:
            table_id: Table ID
            
        Returns:
            Dictionary with table metadata
        """
        try:
            cmd = [
                'bq', 'show',
                '--format=json',
                f'{self.project_id}:{self.dataset_id}.{table_id}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                raise RuntimeError(f"bq show failed for {table_id}: {result.stderr}")
            
            table_data = json.loads(result.stdout)
            
            metadata = {
                "table_id": table_data['tableReference']['tableId'],
                "dataset_id": table_data['tableReference']['datasetId'],
                "project_id": table_data['tableReference']['projectId'],
                "description": table_data.get('description'),
                "num_rows": table_data.get('numRows'),
                "num_bytes": table_data.get('numBytes'),
                "created": table_data.get('creationTime'),
                "modified": table_data.get('lastModifiedTime'),
                "table_type": table_data.get('type', 'TABLE'),
                "labels": table_data.get('labels', {}),
                "location": table_data.get('location'),
            }
            
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to get metadata for table {table_id}: {e}")
            raise RuntimeError(f"Failed to get table metadata: {e}")
