"""Configuration management for BigQuery to ERD tool."""

import os
import json
from typing import Optional, Dict, Any
from pathlib import Path
from dotenv import load_dotenv

from .models import ERDConfig, CustomRulesConfig


class Config:
    """Configuration manager for the BigQuery to ERD tool."""
    
    def __init__(self, env_file: Optional[str] = None):
        """Initialize configuration.
        
        Args:
            env_file: Path to .env file. If None, looks for .env in current directory.
        """
        if env_file:
            load_dotenv(env_file)
        else:
            # Look for .env file in current directory and parent directories
            current_dir = Path.cwd()
            for parent in [current_dir] + list(current_dir.parents):
                env_path = parent / ".env"
                if env_path.exists():
                    load_dotenv(env_path)
                    break
    
    def get_erd_config(self, **overrides) -> ERDConfig:
        """Get ERD configuration from environment variables.
        
        Args:
            **overrides: Configuration overrides
            
        Returns:
            ERDConfig instance
        """
        config_data = {
            "project_id": self._get_env("PROJECT_ID", required=True),
            "dataset_id": self._get_env("DATASET_ID", required=True),
            "location": self._get_env("LOCATION", default="US"),
            "max_results": int(self._get_env("MAX_RESULTS", default="1000")),
            "output_format": self._get_env("OUTPUT_FORMAT", default="drawio"),
            "output_file": self._get_env("OUTPUT_FILE", default="erd_output.drawio"),
            "include_views": self._get_bool_env("INCLUDE_VIEWS", default=False),
            "include_external_tables": self._get_bool_env("INCLUDE_EXTERNAL_TABLES", default=False),
            "enable_fk_detection": self._get_bool_env("ENABLE_FK_DETECTION", default=True),
            "enable_naming_convention_detection": self._get_bool_env("ENABLE_NAMING_CONVENTION_DETECTION", default=True),
            "custom_relationship_rules_file": self._get_env("CUSTOM_RELATIONSHIP_RULES_FILE"),
            "drawio_theme": self._get_env("DRAWIO_THEME", default="default"),
            "table_layout": self._get_env("TABLE_LAYOUT", default="auto"),
            "show_column_types": self._get_bool_env("SHOW_COLUMN_TYPES", default=True),
            "show_column_nullable": self._get_bool_env("SHOW_COLUMN_NULLABLE", default=True),
            "show_indexes": self._get_bool_env("SHOW_INDEXES", default=False),
            "log_level": self._get_env("LOG_LEVEL", default="INFO"),
            "log_file": self._get_env("LOG_FILE"),
        }
        
        # Apply overrides
        config_data.update(overrides)
        
        return ERDConfig(**config_data)
    
    def get_custom_rules_config(self, rules_file: Optional[str] = None) -> Optional[CustomRulesConfig]:
        """Load custom relationship rules from JSON file.
        
        Args:
            rules_file: Path to rules file. If None, uses CUSTOM_RELATIONSHIP_RULES_FILE env var.
            
        Returns:
            CustomRulesConfig instance or None if no rules file
        """
        if not rules_file:
            rules_file = self._get_env("CUSTOM_RELATIONSHIP_RULES_FILE")
        
        if not rules_file or not Path(rules_file).exists():
            return None
        
        try:
            with open(rules_file, 'r') as f:
                rules_data = json.load(f)
            return CustomRulesConfig(**rules_data)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            raise ValueError(f"Error loading custom rules file {rules_file}: {e}")
    
    def get_google_credentials_path(self) -> Optional[str]:
        """Get Google Cloud credentials file path.
        
        Returns:
            Path to credentials file or None
        """
        return self._get_env("GOOGLE_APPLICATION_CREDENTIALS")
    
    def _get_env(self, key: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
        """Get environment variable.
        
        Args:
            key: Environment variable name
            default: Default value if not set
            required: Whether the variable is required
            
        Returns:
            Environment variable value
            
        Raises:
            ValueError: If required variable is not set
        """
        value = os.getenv(key, default)
        if required and value is None:
            raise ValueError(f"Required environment variable {key} is not set")
        return value
    
    def _get_bool_env(self, key: str, default: bool = False) -> bool:
        """Get boolean environment variable.
        
        Args:
            key: Environment variable name
            default: Default value if not set
            
        Returns:
            Boolean value
        """
        value = os.getenv(key, str(default)).lower()
        return value in ('true', '1', 'yes', 'on')
    
    def validate_config(self, config: ERDConfig) -> None:
        """Validate configuration.
        
        Args:
            config: ERDConfig to validate
            
        Raises:
            ValueError: If configuration is invalid
        """
        # Validate required fields
        if not config.project_id:
            raise ValueError("Project ID is required")
        if not config.dataset_id:
            raise ValueError("Dataset ID is required")
        
        # Validate output file path
        output_dir = Path(config.output_file).parent
        if not output_dir.exists():
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                raise ValueError(f"Cannot create output directory {output_dir}: {e}")
        
        # Validate custom rules file if specified
        if config.custom_relationship_rules_file:
            rules_path = Path(config.custom_relationship_rules_file)
            if not rules_path.exists():
                raise ValueError(f"Custom rules file {rules_path} does not exist")
        
        # Validate Google credentials
        creds_path = self.get_google_credentials_path()
        if creds_path and not Path(creds_path).exists():
            raise ValueError(f"Google credentials file {creds_path} does not exist")
