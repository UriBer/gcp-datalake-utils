"""Pydantic data models for BigQuery to ERD tool."""

from typing import List, Optional, Dict, Any, Union
from enum import Enum
from pydantic import BaseModel, Field, validator


class OutputFormat(str, Enum):
    """Supported output formats for ERD generation."""
    DRAWIO = "drawio"
    MERMAID = "mermaid"
    PLANTUML = "plantuml"


class RelationshipType(str, Enum):
    """Types of relationships between tables."""
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class TableLayout(str, Enum):
    """Layout options for table arrangement."""
    AUTO = "auto"
    HORIZONTAL = "horizontal"
    VERTICAL = "vertical"
    FORCE_DIRECTED = "force_directed"
    HIERARCHICAL = "hierarchical"
    GRID = "grid"


class ColumnInfo(BaseModel):
    """Information about a table column."""
    name: str = Field(..., description="Column name")
    data_type: str = Field(..., description="BigQuery data type")
    mode: str = Field(default="NULLABLE", description="Column mode (NULLABLE, REQUIRED, REPEATED)")
    description: Optional[str] = Field(None, description="Column description")
    is_primary_key: bool = Field(default=False, description="Whether this is a primary key")
    is_foreign_key: bool = Field(default=False, description="Whether this is a foreign key")
    max_length: Optional[int] = Field(None, description="Maximum length for string types")
    precision: Optional[int] = Field(None, description="Precision for numeric types")
    scale: Optional[int] = Field(None, description="Scale for numeric types")
    
    @validator('mode')
    def validate_mode(cls, v):
        """Validate column mode."""
        valid_modes = ['NULLABLE', 'REQUIRED', 'REPEATED']
        if v not in valid_modes:
            raise ValueError(f"Mode must be one of {valid_modes}")
        return v


class TableSchema(BaseModel):
    """Schema information for a BigQuery table."""
    table_id: str = Field(..., description="Table ID")
    dataset_id: str = Field(..., description="Dataset ID")
    project_id: str = Field(..., description="Project ID")
    description: Optional[str] = Field(None, description="Table description")
    columns: List[ColumnInfo] = Field(default_factory=list, description="Table columns")
    num_rows: Optional[int] = Field(None, description="Number of rows in table")
    num_bytes: Optional[int] = Field(None, description="Size in bytes")
    created: Optional[str] = Field(None, description="Creation timestamp")
    modified: Optional[str] = Field(None, description="Last modified timestamp")
    table_type: str = Field(default="TABLE", description="Table type (TABLE, VIEW, EXTERNAL)")
    labels: Dict[str, str] = Field(default_factory=dict, description="Table labels")
    
    @property
    def full_table_id(self) -> str:
        """Get the full table ID in format project.dataset.table."""
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"
    
    @property
    def primary_keys(self) -> List[ColumnInfo]:
        """Get all primary key columns."""
        return [col for col in self.columns if col.is_primary_key]
    
    @property
    def foreign_keys(self) -> List[ColumnInfo]:
        """Get all foreign key columns."""
        return [col for col in self.columns if col.is_foreign_key]


class Relationship(BaseModel):
    """Relationship between two tables."""
    source_table: str = Field(..., description="Source table ID")
    source_column: str = Field(..., description="Source column name")
    target_table: str = Field(..., description="Target table ID")
    target_column: str = Field(..., description="Target column name")
    relationship_type: RelationshipType = Field(..., description="Type of relationship")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score (0-1)")
    detection_method: str = Field(..., description="Method used to detect this relationship")
    is_custom: bool = Field(default=False, description="Whether this is a custom relationship")
    
    @property
    def source_full_id(self) -> str:
        """Get full source table ID."""
        return self.source_table
    
    @property
    def target_full_id(self) -> str:
        """Get full target table ID."""
        return self.target_table


class ERDConfig(BaseModel):
    """Configuration for ERD generation."""
    # BigQuery settings
    project_id: str = Field(..., description="GCP project ID")
    dataset_id: str = Field(..., description="BigQuery dataset ID")
    location: str = Field(default="US", description="BigQuery location")
    max_results: int = Field(default=1000, description="Maximum number of tables to process")
    
    # Output settings
    output_format: OutputFormat = Field(default=OutputFormat.DRAWIO, description="Output format")
    output_file: str = Field(default="erd_output.drawio", description="Output file path")
    include_views: bool = Field(default=False, description="Include views in ERD")
    include_external_tables: bool = Field(default=False, description="Include external tables")
    
    # Relationship detection
    enable_fk_detection: bool = Field(default=True, description="Enable foreign key detection")
    enable_naming_convention_detection: bool = Field(default=True, description="Enable naming convention detection")
    custom_relationship_rules_file: Optional[str] = Field(None, description="Path to custom rules file")
    
    # Draw.io specific settings
    drawio_theme: str = Field(default="default", description="Draw.io theme")
    table_layout: TableLayout = Field(default=TableLayout.AUTO, description="Table layout algorithm")
    show_column_types: bool = Field(default=True, description="Show column data types")
    show_column_nullable: bool = Field(default=True, description="Show nullable indicators")
    show_indexes: bool = Field(default=False, description="Show indexes")
    
    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    log_file: Optional[str] = Field(None, description="Log file path")
    
    @validator('log_level')
    def validate_log_level(cls, v):
        """Validate log level."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v.upper()
    
    @validator('drawio_theme')
    def validate_drawio_theme(cls, v):
        """Validate Draw.io theme."""
        valid_themes = ['default', 'dark', 'minimal']
        if v not in valid_themes:
            raise ValueError(f"Draw.io theme must be one of {valid_themes}")
        return v


class CustomRelationshipRule(BaseModel):
    """Custom relationship rule definition."""
    source_table: str = Field(..., description="Source table pattern")
    source_column: str = Field(..., description="Source column pattern")
    target_table: str = Field(..., description="Target table pattern")
    target_column: str = Field(..., description="Target column pattern")
    relationship_type: RelationshipType = Field(..., description="Relationship type")
    confidence: float = Field(default=0.9, ge=0.0, le=1.0, description="Confidence score")


class NamingPattern(BaseModel):
    """Naming pattern for relationship detection."""
    pattern: str = Field(..., description="Regex pattern for column names")
    target_suffix: str = Field(default="", description="Target table suffix")
    confidence: float = Field(default=0.8, ge=0.0, le=1.0, description="Confidence score")


class CustomRulesConfig(BaseModel):
    """Configuration for custom relationship rules."""
    relationships: List[CustomRelationshipRule] = Field(default_factory=list)
    naming_patterns: List[NamingPattern] = Field(default_factory=list)
