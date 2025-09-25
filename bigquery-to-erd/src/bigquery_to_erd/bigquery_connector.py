"""BigQuery connector for extracting schema information."""

import logging
from typing import List, Optional, Dict, Any, Iterator
from pathlib import Path
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError
from google.oauth2 import service_account

from .models import TableSchema, ColumnInfo, ERDConfig


logger = logging.getLogger(__name__)


class BigQueryConnector:
    """Connector for BigQuery operations."""
    
    def __init__(self, config: ERDConfig, credentials_path: Optional[str] = None):
        """Initialize BigQuery connector.
        
        Args:
            config: ERD configuration
            credentials_path: Path to service account credentials file
        """
        self.config = config
        self.client: Optional[bigquery.Client] = None
        self.credentials_path = credentials_path
    
    def connect(self) -> None:
        """Establish connection to BigQuery.
        
        Raises:
            GoogleCloudError: If connection fails
        """
        try:
            if self.credentials_path and Path(self.credentials_path).exists():
                logger.info(f"Using service account credentials: {self.credentials_path}")
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path
                )
                self.client = bigquery.Client(
                    project=self.config.project_id,
                    credentials=credentials,
                    location=self.config.location
                )
            else:
                # Use default credentials (ADC, gcloud CLI, environment, etc.)
                logger.info("Using Application Default Credentials (gcloud CLI or environment)")
                self.client = bigquery.Client(
                    project=self.config.project_id,
                    location=self.config.location
                )
            
            logger.info(f"Connected to BigQuery project: {self.config.project_id}")
            
        except Exception as e:
            logger.error(f"Failed to connect to BigQuery: {e}")
            if "credentials" in str(e).lower():
                logger.error("Authentication failed. Try running: gcloud auth application-default login")
            raise GoogleCloudError(f"Connection failed: {e}")
    
    def list_tables(self, dataset_id: Optional[str] = None) -> List[str]:
        """List all tables in the dataset.
        
        Args:
            dataset_id: Dataset ID. If None, uses config dataset_id.
            
        Returns:
            List of table IDs
            
        Raises:
            NotFound: If dataset doesn't exist
            GoogleCloudError: If listing fails
        """
        if not self.client:
            raise RuntimeError("Not connected to BigQuery. Call connect() first.")
        
        dataset_id = dataset_id or self.config.dataset_id
        dataset_ref = self.client.dataset(dataset_id)
        
        try:
            tables = list(self.client.list_tables(dataset_ref, max_results=self.config.max_results))
            table_ids = [table.table_id for table in tables]
            
            logger.info(f"Found {len(table_ids)} tables in dataset {dataset_id}")
            return table_ids
            
        except NotFound:
            logger.error(f"Dataset {dataset_id} not found")
            raise
        except Exception as e:
            logger.error(f"Failed to list tables in dataset {dataset_id}: {e}")
            raise GoogleCloudError(f"Failed to list tables: {e}")
    
    def get_table_schema(self, table_id: str, dataset_id: Optional[str] = None) -> TableSchema:
        """Get schema information for a table.
        
        Args:
            table_id: Table ID
            dataset_id: Dataset ID. If None, uses config dataset_id.
            
        Returns:
            TableSchema object
            
        Raises:
            NotFound: If table doesn't exist
            GoogleCloudError: If schema extraction fails
        """
        if not self.client:
            raise RuntimeError("Not connected to BigQuery. Call connect() first.")
        
        dataset_id = dataset_id or self.config.dataset_id
        table_ref = self.client.dataset(dataset_id).table(table_id)
        
        try:
            table = self.client.get_table(table_ref)
            
            # Extract column information
            columns = []
            for field in table.schema:
                column_info = ColumnInfo(
                    name=field.name,
                    data_type=field.field_type,
                    mode=field.mode,
                    description=field.description,
                    max_length=field.max_length,
                    precision=field.precision,
                    scale=field.scale
                )
                columns.append(column_info)
            
            # Create TableSchema
            table_schema = TableSchema(
                table_id=table.table_id,
                dataset_id=table.dataset_id,
                project_id=table.project,
                description=table.description,
                columns=columns,
                num_rows=table.num_rows,
                num_bytes=table.num_bytes,
                created=table.created.isoformat() if table.created else None,
                modified=table.modified.isoformat() if table.modified else None,
                table_type=table.table_type,
                labels=dict(table.labels) if table.labels else {}
            )
            
            logger.debug(f"Extracted schema for table {table_id}: {len(columns)} columns")
            return table_schema
            
        except NotFound:
            logger.error(f"Table {table_id} not found in dataset {dataset_id}")
            raise
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_id}: {e}")
            raise GoogleCloudError(f"Failed to get table schema: {e}")
    
    def get_table_metadata(self, table_id: str, dataset_id: Optional[str] = None) -> Dict[str, Any]:
        """Get metadata for a table.
        
        Args:
            table_id: Table ID
            dataset_id: Dataset ID. If None, uses config dataset_id.
            
        Returns:
            Dictionary with table metadata
        """
        if not self.client:
            raise RuntimeError("Not connected to BigQuery. Call connect() first.")
        
        dataset_id = dataset_id or self.config.dataset_id
        table_ref = self.client.dataset(dataset_id).table(table_id)
        
        try:
            table = self.client.get_table(table_ref)
            
            metadata = {
                "table_id": table.table_id,
                "dataset_id": table.dataset_id,
                "project_id": table.project,
                "description": table.description,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None,
                "table_type": table.table_type,
                "labels": dict(table.labels) if table.labels else {},
                "expires": table.expires.isoformat() if table.expires else None,
                "location": table.location,
                "clustering_fields": table.clustering_fields,
                "time_partitioning": {
                    "type": table.time_partitioning.type if table.time_partitioning else None,
                    "field": table.time_partitioning.field if table.time_partitioning else None,
                    "expiration_ms": table.time_partitioning.expiration_ms if table.time_partitioning else None,
                } if table.time_partitioning else None,
            }
            
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to get metadata for table {table_id}: {e}")
            raise GoogleCloudError(f"Failed to get table metadata: {e}")
    
    def get_all_table_schemas(self, dataset_id: Optional[str] = None) -> List[TableSchema]:
        """Get schemas for all tables in the dataset.
        
        Args:
            dataset_id: Dataset ID. If None, uses config dataset_id.
            
        Returns:
            List of TableSchema objects
        """
        dataset_id = dataset_id or self.config.dataset_id
        table_ids = self.list_tables(dataset_id)
        
        schemas = []
        for table_id in table_ids:
            try:
                # Check if we should include this table type
                table_ref = self.client.dataset(dataset_id).table(table_id)
                table = self.client.get_table(table_ref)
                
                should_include = True
                if table.table_type == "VIEW" and not self.config.include_views:
                    should_include = False
                elif table.table_type == "EXTERNAL" and not self.config.include_external_tables:
                    should_include = False
                
                if should_include:
                    schema = self.get_table_schema(table_id, dataset_id)
                    schemas.append(schema)
                else:
                    logger.debug(f"Skipping {table.table_type} table: {table_id}")
                    
            except Exception as e:
                logger.warning(f"Failed to get schema for table {table_id}: {e}")
                continue
        
        logger.info(f"Successfully extracted schemas for {len(schemas)} tables")
        return schemas
    
    def test_connection(self) -> bool:
        """Test the BigQuery connection.
        
        Returns:
            True if connection is working, False otherwise
        """
        try:
            if not self.client:
                return False
            
            # Try to list datasets to test connection
            datasets = list(self.client.list_datasets(max_results=1))
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def close(self) -> None:
        """Close the BigQuery connection."""
        if self.client:
            self.client.close()
            self.client = None
            logger.info("BigQuery connection closed")
