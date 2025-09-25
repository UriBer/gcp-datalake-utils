"""BigQuery to ERD Tool - Generate Entity Relationship Diagrams from BigQuery datasets."""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from .models import TableSchema, ColumnInfo, Relationship, ERDConfig
from .bigquery_connector import BigQueryConnector
from .schema_analyzer import SchemaAnalyzer
from .relationship_detector import RelationshipDetector
from .erd_generator import ERDGenerator

__all__ = [
    "TableSchema",
    "ColumnInfo", 
    "Relationship",
    "ERDConfig",
    "BigQueryConnector",
    "SchemaAnalyzer",
    "RelationshipDetector",
    "ERDGenerator",
]
