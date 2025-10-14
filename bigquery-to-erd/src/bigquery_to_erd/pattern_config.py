"""Configuration loader for relationship patterns and naming conventions."""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass


@dataclass
class TablePattern:
    """Represents a table naming pattern."""
    prefix: str
    description: str
    primary_key_patterns: List[str]
    foreign_key_patterns: List[str]
    relationship_rules: Dict[str, Any]


@dataclass
class DetectionStrategy:
    """Represents a relationship detection strategy."""
    name: str
    description: str
    confidence: float
    rules: List[Dict[str, Any]]


@dataclass
class PatternConfig:
    """Main configuration class for patterns and relationships."""
    table_patterns: Dict[str, Dict[str, TablePattern]]
    detection_strategies: List[DetectionStrategy]
    column_patterns: Dict[str, Any]
    confidence_scoring: Dict[str, float]
    filtering_rules: Dict[str, Any]


class PatternConfigLoader:
    """Loads and manages pattern configuration from JSON files."""
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize the pattern config loader.
        
        Args:
            config_file: Path to the configuration file. If None, uses default.
        """
        if config_file is None:
            # Default to the config file in the project
            config_file = Path(__file__).parent.parent.parent / "config" / "relationship_patterns.json"
        
        self.config_file = Path(config_file)
        self.config = self._load_config()
    
    def _load_config(self) -> PatternConfig:
        """Load configuration from JSON file.
        
        Returns:
            PatternConfig object
        """
        if not self.config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
        
        with open(self.config_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Parse table patterns
        table_patterns = {}
        for methodology, patterns in data.get("table_patterns", {}).items():
            table_patterns[methodology] = {}
            for pattern_name, pattern_data in patterns.get("patterns", {}).items():
                table_patterns[methodology][pattern_name] = TablePattern(
                    prefix=pattern_data.get("prefix", ""),
                    description=pattern_data.get("description", ""),
                    primary_key_patterns=pattern_data.get("primary_key_patterns", []),
                    foreign_key_patterns=pattern_data.get("foreign_key_patterns", []),
                    relationship_rules=pattern_data.get("relationship_rules", {})
                )
        
        # Parse detection strategies
        detection_strategies = []
        for strategy_data in data.get("relationship_detection", {}).get("strategies", []):
            detection_strategies.append(DetectionStrategy(
                name=strategy_data.get("name", ""),
                description=strategy_data.get("description", ""),
                confidence=strategy_data.get("confidence", 0.5),
                rules=strategy_data.get("rules", [])
            ))
        
        return PatternConfig(
            table_patterns=table_patterns,
            detection_strategies=detection_strategies,
            column_patterns=data.get("column_patterns", {}),
            confidence_scoring=data.get("confidence_scoring", {}),
            filtering_rules=data.get("filtering_rules", {})
        )
    
    def get_table_pattern(self, methodology: str, pattern_name: str) -> Optional[TablePattern]:
        """Get a specific table pattern.
        
        Args:
            methodology: The methodology (e.g., 'data_vault', 'traditional_dw')
            pattern_name: The pattern name (e.g., 'hub', 'dimension')
            
        Returns:
            TablePattern or None if not found
        """
        return self.config.table_patterns.get(methodology, {}).get(pattern_name)
    
    def get_patterns_for_table(self, table_name: str) -> List[Tuple[str, str, TablePattern]]:
        """Get all matching patterns for a table name.
        
        Args:
            table_name: The table name to match
            
        Returns:
            List of (methodology, pattern_name, pattern) tuples
        """
        matches = []
        table_name_lower = table_name.lower()
        
        for methodology, patterns in self.config.table_patterns.items():
            for pattern_name, pattern in patterns.items():
                if table_name_lower.startswith(pattern.prefix):
                    matches.append((methodology, pattern_name, pattern))
        
        return matches
    
    def is_primary_key_candidate(self, column_name: str, table_name: str) -> bool:
        """Check if a column is a primary key candidate based on patterns.
        
        Args:
            column_name: The column name
            table_name: The table name
            
        Returns:
            True if the column is likely a primary key
        """
        column_name_lower = column_name.lower()
        
        # Check against global primary key indicators
        global_pk_indicators = self.config.column_patterns.get("primary_key_indicators", [])
        for indicator in global_pk_indicators:
            if self._matches_pattern(column_name_lower, indicator):
                return True
        
        # Check against table-specific patterns
        patterns = self.get_patterns_for_table(table_name)
        for methodology, pattern_name, pattern in patterns:
            for pk_pattern in pattern.primary_key_patterns:
                if self._matches_pattern(column_name_lower, pk_pattern):
                    return True
        
        return False
    
    def is_foreign_key_candidate(self, column_name: str, table_name: str) -> bool:
        """Check if a column is a foreign key candidate based on patterns.
        
        Args:
            column_name: The column name
            table_name: The table name
            
        Returns:
            True if the column is likely a foreign key
        """
        column_name_lower = column_name.lower()
        
        # Check against global foreign key indicators
        global_fk_indicators = self.config.column_patterns.get("foreign_key_indicators", [])
        for indicator in global_fk_indicators:
            if self._matches_pattern(column_name_lower, indicator):
                return True
        
        # Check against table-specific patterns
        patterns = self.get_patterns_for_table(table_name)
        for methodology, pattern_name, pattern in patterns:
            for fk_pattern in pattern.foreign_key_patterns:
                if self._matches_pattern(column_name_lower, fk_pattern):
                    return True
        
        return False
    
    def find_target_table(self, column_name: str, available_tables: List[str]) -> Optional[str]:
        """Find target table for a foreign key column.
        
        Args:
            column_name: The foreign key column name
            available_tables: List of available table names
            
        Returns:
            Target table name or None if not found
        """
        column_name_lower = column_name.lower()
        available_tables_lower = [t.lower() for t in available_tables]
        
        # Try each detection strategy
        for strategy in self.config.detection_strategies:
            target = self._apply_strategy(strategy, column_name_lower, available_tables_lower)
            if target:
                # Find the original case version
                for table in available_tables:
                    if table.lower() == target:
                        return table
        
        return None
    
    def _apply_strategy(self, strategy: DetectionStrategy, column_name: str, available_tables: List[str]) -> Optional[str]:
        """Apply a detection strategy to find target table.
        
        Args:
            strategy: The detection strategy
            column_name: The column name
            available_tables: List of available table names (lowercase)
            
        Returns:
            Target table name (lowercase) or None
        """
        for rule in strategy.rules:
            if rule.get("pattern") == "remove_suffixes":
                suffixes = rule.get("suffixes", [])
                base_name = self._remove_suffixes(column_name, suffixes)
                if base_name in available_tables:
                    return base_name
                
                # Try with prefixes
                for prefix in ["h_", "dim_", "l_", "ref_", "fact_", "tbl_", "table_"]:
                    prefixed_name = f"{prefix}{base_name}"
                    if prefixed_name in available_tables:
                        return prefixed_name
            
            elif rule.get("pattern") == "data_vault_hub_reference":
                if column_name.endswith("_hk") or column_name.endswith("_hash_key"):
                    hub_name = re.sub(r"_(hk|hash_key)$", "", column_name)
                    hub_table = f"h_{hub_name}"
                    if hub_table in available_tables:
                        return hub_table
            
            elif rule.get("pattern") == "plural_singular":
                transformations = rule.get("transformations", [])
                base_name = self._remove_suffixes(column_name, ["_id", "_key", "_fk", "_pk", "_hk", "_hash_key"])
                
                for transform in transformations:
                    if transform == "add_s":
                        candidate = base_name + "s"
                    elif transform == "add_es":
                        candidate = base_name + "es"
                    elif transform == "remove_s":
                        candidate = base_name.rstrip("s")
                    else:
                        continue
                    
                    if candidate in available_tables:
                        return candidate
                    
                    # Try with prefixes
                    for prefix in ["h_", "dim_", "l_", "ref_", "fact_", "tbl_", "table_"]:
                        prefixed_candidate = f"{prefix}{candidate}"
                        if prefixed_candidate in available_tables:
                            return prefixed_candidate
        
        return None
    
    def _matches_pattern(self, text: str, pattern: str) -> bool:
        """Check if text matches a pattern.
        
        Args:
            text: The text to match
            pattern: The pattern (supports wildcards with *)
            
        Returns:
            True if text matches pattern
        """
        if "*" in pattern:
            # Convert wildcard pattern to regex
            regex_pattern = pattern.replace("*", ".*")
            return bool(re.match(f"^{regex_pattern}$", text, re.IGNORECASE))
        else:
            return text == pattern
    
    def _remove_suffixes(self, text: str, suffixes: List[str]) -> str:
        """Remove suffixes from text.
        
        Args:
            text: The text to process
            suffixes: List of suffixes to remove
            
        Returns:
            Text with suffixes removed
        """
        for suffix in suffixes:
            if text.endswith(suffix):
                return text[:-len(suffix)]
        return text
    
    def get_confidence_score(self, detection_method: str) -> float:
        """Get confidence score for a detection method.
        
        Args:
            detection_method: The detection method name
            
        Returns:
            Confidence score
        """
        return self.config.confidence_scoring.get(detection_method, 0.5)
    
    def get_filtering_rules(self) -> Dict[str, Any]:
        """Get filtering rules for relationships.
        
        Returns:
            Dictionary of filtering rules
        """
        return self.config.filtering_rules
