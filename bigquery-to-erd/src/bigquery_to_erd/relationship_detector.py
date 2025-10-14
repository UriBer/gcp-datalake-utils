"""Relationship detection engine for BigQuery tables."""

import logging
import re
import json
from typing import List, Dict, Set, Optional, Tuple
from pathlib import Path
from collections import defaultdict

from .models import (
    TableSchema, ColumnInfo, Relationship, RelationshipType, 
    CustomRulesConfig, CustomRelationshipRule, NamingPattern
)


logger = logging.getLogger(__name__)


class RelationshipDetector:
    """Detects relationships between BigQuery tables."""
    
    def __init__(self, config: Optional[CustomRulesConfig] = None):
        """Initialize relationship detector.
        
        Args:
            config: Custom rules configuration
        """
        self.custom_rules = config
        self.detection_methods = {
            "foreign_key": self._detect_foreign_keys,
            "naming_convention": self._detect_naming_conventions,
            "data_type_match": self._detect_data_type_matches,
            "custom_rules": self._apply_custom_rules,
        }
    
    def detect_relationships(self, tables: List[TableSchema], 
                           enable_fk_detection: bool = True,
                           enable_naming_convention_detection: bool = True) -> List[Relationship]:
        """Detect relationships between tables using multiple methods.
        
        Args:
            tables: List of table schemas
            enable_fk_detection: Enable foreign key detection
            enable_naming_convention_detection: Enable naming convention detection
            
        Returns:
            List of detected relationships
        """
        all_relationships = []
        
        # Create table lookup for efficient access
        table_map = {table.table_id: table for table in tables}
        
        # Apply different detection methods
        if enable_fk_detection:
            fk_relationships = self._detect_foreign_keys(tables, table_map)
            all_relationships.extend(fk_relationships)
        
        if enable_naming_convention_detection:
            naming_relationships = self._detect_naming_conventions(tables, table_map)
            all_relationships.extend(naming_relationships)
        
        # Enhanced PK-FK detection with better patterns
        enhanced_relationships = self._detect_enhanced_pk_fk_relationships(tables, table_map)
        all_relationships.extend(enhanced_relationships)
        
        # Always apply data type matching
        type_relationships = self._detect_data_type_matches(tables, table_map)
        all_relationships.extend(type_relationships)
        
        # Apply custom rules if available
        if self.custom_rules:
            custom_relationships = self._apply_custom_rules(tables, table_map)
            all_relationships.extend(custom_relationships)
        
        # Remove duplicates and resolve conflicts
        unique_relationships = self._resolve_relationship_conflicts(all_relationships)
        
        # Filter relationships to reduce clutter
        filtered_relationships = self._filter_relationships(unique_relationships)
        
        logger.info(f"Detected {len(unique_relationships)} unique relationships, filtered to {len(filtered_relationships)} meaningful relationships")
        return filtered_relationships
    
    def _detect_foreign_keys(self, tables: List[TableSchema], 
                           table_map: Dict[str, TableSchema]) -> List[Relationship]:
        """Detect foreign key relationships.
        
        Args:
            tables: List of table schemas
            table_map: Map of table_id to TableSchema
            
        Returns:
            List of foreign key relationships
        """
        relationships = []
        
        for table in tables:
            for column in table.columns:
                if column.is_foreign_key:
                    # Try to find target table and column
                    target_info = self._find_foreign_key_target(column, table_map)
                    if target_info:
                        target_table, target_column = target_info
                        relationship = Relationship(
                            source_table=table.table_id,
                            source_column=column.name,
                            target_table=target_table.table_id,
                            target_column=target_column.name,
                            relationship_type=RelationshipType.MANY_TO_ONE,
                            confidence=0.8,
                            detection_method="foreign_key"
                        )
                        relationships.append(relationship)
        
        logger.debug(f"Detected {len(relationships)} foreign key relationships")
        return relationships
    
    def _detect_enhanced_pk_fk_relationships(self, tables: List[TableSchema], 
                                           table_map: Dict[str, TableSchema]) -> List[Relationship]:
        """Enhanced PK-FK relationship detection with better naming patterns.
        
        Args:
            tables: List of table schemas
            table_map: Map of table_id to TableSchema
            
        Returns:
            List of enhanced PK-FK relationships
        """
        relationships = []
        
        # Build a map of potential primary keys by table
        pk_map = {}
        for table in tables:
            # Find primary key columns
            primary_keys = []
            for column in table.columns:
                if (column.is_primary_key or 
                    self._is_likely_primary_key(column.name, table.table_id)):
                    primary_keys.append(column)
            
            # If no explicit PKs found, look for common PK patterns
            if not primary_keys:
                for column in table.columns:
                    if self._is_common_primary_key(column.name):
                        primary_keys.append(column)
            
            pk_map[table.table_id] = primary_keys
        
        # Look for foreign key patterns
        for table in tables:
            for column in table.columns:
                if column.is_foreign_key:
                    continue
                
                # Try to find matching primary key
                target_info = self._find_enhanced_target(table, column, pk_map, table_map)
                if target_info:
                    target_table, target_column = target_info
                    relationship = Relationship(
                        source_table=table.table_id,
                        source_column=column.name,
                        target_table=target_table.table_id,
                        target_column=target_column.name,
                        relationship_type=RelationshipType.MANY_TO_ONE,
                        confidence=0.9,  # High confidence for enhanced detection
                        detection_method="enhanced_pk_fk"
                    )
                    relationships.append(relationship)
        
        logger.debug(f"Detected {len(relationships)} enhanced PK-FK relationships")
        return relationships
    
    def _is_likely_primary_key(self, column_name: str, table_name: str) -> bool:
        """Check if column is likely a primary key based on naming.
        
        Args:
            column_name: Column name
            table_name: Table name
            
        Returns:
            True if likely primary key
        """
        # Common PK patterns
        pk_patterns = [
            r'^id$',
            r'^.*_id$',
            r'^.*_key$',
            r'^.*_pk$',
            r'^pk_.*$',
            r'^.*_code$',
            r'^.*_number$',
        ]
        
        for pattern in pk_patterns:
            if re.match(pattern, column_name, re.IGNORECASE):
                return True
        
        # Check if column name matches table name pattern
        table_base = table_name.lower().replace('dim_', '').replace('fact_', '')
        if column_name.lower() in [f"{table_base}_id", f"{table_base}_key", "id"]:
            return True
        
        return False
    
    def _is_common_primary_key(self, column_name: str) -> bool:
        """Check if column is a common primary key name.
        
        Args:
            column_name: Column name
            
        Returns:
            True if common PK name
        """
        common_pk_names = ['id', 'key', 'pk', 'code', 'number', 'identifier']
        return column_name.lower() in common_pk_names
    
    def _find_enhanced_target(self, source_table: TableSchema, source_column: ColumnInfo,
                            pk_map: Dict[str, List[ColumnInfo]], 
                            table_map: Dict[str, TableSchema]) -> Optional[Tuple[TableSchema, ColumnInfo]]:
        """Find target table and column for enhanced PK-FK detection.
        
        Args:
            source_table: Source table
            source_column: Source column
            pk_map: Map of table_id to primary keys
            table_map: Map of table_id to TableSchema
            
        Returns:
            Tuple of (target_table, target_column) or None
        """
        # Strategy 1: Direct name matching
        target_table = self._find_target_by_direct_name(source_column.name, table_map)
        if target_table:
            target_column = self._find_best_primary_key(target_table, pk_map.get(target_table.table_id, []))
            if target_column and self._are_columns_compatible(source_column, target_column):
                return (target_table, target_column)
        
        # Strategy 2: Pattern-based matching
        target_table = self._find_target_by_pattern(source_column.name, table_map)
        if target_table:
            target_column = self._find_best_primary_key(target_table, pk_map.get(target_table.table_id, []))
            if target_column and self._are_columns_compatible(source_column, target_column):
                return (target_table, target_column)
        
        # Strategy 3: Data type matching
        for table_id, primary_keys in pk_map.items():
            if table_id == source_table.table_id:
                continue
            
            for pk_column in primary_keys:
                if self._are_columns_compatible(source_column, pk_column):
                    return (table_map[table_id], pk_column)
        
        return None
    
    def _find_target_by_direct_name(self, column_name: str, table_map: Dict[str, TableSchema]) -> Optional[TableSchema]:
        """Find target table by direct name matching.
        
        Args:
            column_name: Column name
            table_map: Map of table_id to TableSchema
            
        Returns:
            Target table or None
        """
        # Remove common suffixes
        base_name = re.sub(r'_(id|key|fk|pk)$', '', column_name, flags=re.IGNORECASE)
        
        # Try exact match
        if base_name in table_map:
            return table_map[base_name]
        
        # Try with common prefixes
        for prefix in ['dim_', 'fact_', 'tbl_', 'table_']:
            prefixed_name = f"{prefix}{base_name}"
            if prefixed_name in table_map:
                return table_map[prefixed_name]
        
        return None
    
    def _find_target_by_pattern(self, column_name: str, table_map: Dict[str, TableSchema]) -> Optional[TableSchema]:
        """Find target table by pattern matching.
        
        Args:
            column_name: Column name
            table_map: Map of table_id to TableSchema
            
        Returns:
            Target table or None
        """
        # Extract base name from column
        base_name = re.sub(r'_(id|key|fk|pk)$', '', column_name, flags=re.IGNORECASE)
        
        # Try different transformations
        candidates = [
            base_name,
            base_name + 's',  # plural
            base_name + 'es',  # plural with es
            base_name.rstrip('s'),  # singular
        ]
        
        for candidate in candidates:
            if candidate in table_map:
                return table_map[candidate]
            
            # Try with prefixes
            for prefix in ['dim_', 'fact_', 'tbl_', 'table_']:
                prefixed_candidate = f"{prefix}{candidate}"
                if prefixed_candidate in table_map:
                    return table_map[prefixed_candidate]
        
        return None
    
    def _find_best_primary_key(self, table: TableSchema, primary_keys: List[ColumnInfo]) -> Optional[ColumnInfo]:
        """Find the best primary key from a list of candidates.
        
        Args:
            table: Target table
            primary_keys: List of primary key candidates
            
        Returns:
            Best primary key or None
        """
        if not primary_keys:
            return None
        
        # Prefer explicitly marked primary keys
        explicit_pks = [pk for pk in primary_keys if pk.is_primary_key]
        if explicit_pks:
            return explicit_pks[0]
        
        # Prefer 'id' columns
        id_columns = [pk for pk in primary_keys if pk.name.lower() == 'id']
        if id_columns:
            return id_columns[0]
        
        # Return first candidate
        return primary_keys[0]
    
    def _are_columns_compatible(self, col1: ColumnInfo, col2: ColumnInfo) -> bool:
        """Check if two columns are compatible for a relationship.
        
        Args:
            col1: First column
            col2: Second column
            
        Returns:
            True if columns are compatible
        """
        # Must have same data type
        if col1.data_type != col2.data_type:
            return False
        
        # Both should be required or both nullable
        if col1.mode != col2.mode and not (col1.mode == "NULLABLE" and col2.mode == "NULLABLE"):
            return False
        
        return True
    
    def _detect_naming_conventions(self, tables: List[TableSchema],
                                 table_map: Dict[str, TableSchema]) -> List[Relationship]:
        """Detect relationships based on naming conventions.
        
        Args:
            tables: List of table schemas
            table_map: Map of table_id to TableSchema
            
        Returns:
            List of naming convention relationships
        """
        relationships = []
        
        # Common naming patterns
        patterns = [
            # user_id -> users.id
            (r'^(.+)_id$', lambda m: m.group(1) + 's'),
            # customer_id -> customers.id
            (r'^(.+)_id$', lambda m: m.group(1) + 's'),
            # order_id -> orders.id
            (r'^(.+)_id$', lambda m: m.group(1) + 's'),
        ]
        
        for table in tables:
            for column in table.columns:
                # Skip if already identified as FK
                if column.is_foreign_key:
                    continue
                
                for pattern, transform in patterns:
                    match = re.match(pattern, column.name, re.IGNORECASE)
                    if match:
                        target_table_name = transform(match)
                        if target_table_name in table_map:
                            target_table = table_map[target_table_name]
                            target_column = self._find_best_target_column(target_table, column)
                            if target_column:
                                relationship = Relationship(
                                    source_table=table.table_id,
                                    source_column=column.name,
                                    target_table=target_table.table_id,
                                    target_column=target_column.name,
                                    relationship_type=RelationshipType.MANY_TO_ONE,
                                    confidence=0.6,
                                    detection_method="naming_convention"
                                )
                                relationships.append(relationship)
        
        logger.debug(f"Detected {len(relationships)} naming convention relationships")
        return relationships
    
    def _detect_data_type_matches(self, tables: List[TableSchema],
                                table_map: Dict[str, TableSchema]) -> List[Relationship]:
        """Detect relationships based on compatible data types.
        
        Args:
            tables: List of table schemas
            table_map: Map of table_id to TableSchema
            
        Returns:
            List of data type match relationships
        """
        relationships = []
        
        # Group columns by data type for efficient matching
        type_groups = defaultdict(list)
        for table in tables:
            for column in table.columns:
                if not column.is_primary_key:  # Skip PKs to avoid self-references
                    type_groups[column.data_type].append((table, column))
        
        # Find potential matches within same data types
        for data_type, columns in type_groups.items():
            if len(columns) < 2:
                continue
            
            # Look for potential relationships
            for i, (source_table, source_column) in enumerate(columns):
                for j, (target_table, target_column) in enumerate(columns[i+1:], i+1):
                    # Skip same table
                    if source_table.table_id == target_table.table_id:
                        continue
                    
                    # Check if this looks like a relationship
                    if self._is_potential_relationship(source_column, target_column):
                        # Determine relationship type
                        rel_type = self._determine_relationship_type(
                            source_table, source_column, target_table, target_column
                        )
                        
                        relationship = Relationship(
                            source_table=source_table.table_id,
                            source_column=source_column.name,
                            target_table=target_table.table_id,
                            target_column=target_column.name,
                            relationship_type=rel_type,
                            confidence=0.4,  # Lower confidence for type-only matching
                            detection_method="data_type_match"
                        )
                        relationships.append(relationship)
        
        logger.debug(f"Detected {len(relationships)} data type match relationships")
        return relationships
    
    def _apply_custom_rules(self, tables: List[TableSchema],
                          table_map: Dict[str, TableSchema]) -> List[Relationship]:
        """Apply custom relationship rules.
        
        Args:
            tables: List of table schemas
            table_map: Map of table_id to TableSchema
            
        Returns:
            List of custom rule relationships
        """
        if not self.custom_rules:
            return []
        
        relationships = []
        
        # Apply explicit relationship rules
        for rule in self.custom_rules.relationships:
            if (rule.source_table in table_map and 
                rule.target_table in table_map):
                
                source_table = table_map[rule.source_table]
                target_table = table_map[rule.target_table]
                
                # Find matching columns
                source_column = self._find_column_by_name(source_table, rule.source_column)
                target_column = self._find_column_by_name(target_table, rule.target_column)
                
                if source_column and target_column:
                    relationship = Relationship(
                        source_table=source_table.table_id,
                        source_column=source_column.name,
                        target_table=target_table.table_id,
                        target_column=target_column.name,
                        relationship_type=rule.relationship_type,
                        confidence=rule.confidence,
                        detection_method="custom_rules",
                        is_custom=True
                    )
                    relationships.append(relationship)
        
        # Apply naming pattern rules
        for pattern_rule in self.custom_rules.naming_patterns:
            pattern_relationships = self._apply_naming_pattern(
                pattern_rule, tables, table_map
            )
            relationships.extend(pattern_relationships)
        
        logger.debug(f"Applied {len(relationships)} custom rule relationships")
        return relationships
    
    def _find_foreign_key_target(self, column: ColumnInfo, 
                               table_map: Dict[str, TableSchema]) -> Optional[Tuple[TableSchema, ColumnInfo]]:
        """Find target table and column for a foreign key.
        
        Args:
            column: Foreign key column
            table_map: Map of table_id to TableSchema
            
        Returns:
            Tuple of (target_table, target_column) or None
        """
        # Try different strategies to find target
        target_table = self._find_target_table_by_name(column.name, table_map)
        if target_table:
            target_column = self._find_best_target_column(target_table, column)
            if target_column:
                return (target_table, target_column)
        
        return None
    
    def _find_target_table_by_name(self, column_name: str, 
                                 table_map: Dict[str, TableSchema]) -> Optional[TableSchema]:
        """Find target table based on column name.
        
        Args:
            column_name: Column name
            table_map: Map of table_id to TableSchema
            
        Returns:
            Target table or None
        """
        # Common patterns
        patterns = [
            (r'^(.+)_id$', lambda m: m.group(1) + 's'),
            (r'^(.+)_id$', lambda m: m.group(1)),
            (r'^(.+)_key$', lambda m: m.group(1) + 's'),
            (r'^(.+)_fk$', lambda m: m.group(1) + 's'),
        ]
        
        for pattern, transform in patterns:
            match = re.match(pattern, column_name, re.IGNORECASE)
            if match:
                target_name = transform(match)
                if target_name in table_map:
                    return table_map[target_name]
        
        return None
    
    def _find_best_target_column(self, target_table: TableSchema, 
                               source_column: ColumnInfo) -> Optional[ColumnInfo]:
        """Find the best target column in target table.
        
        Args:
            target_table: Target table schema
            source_column: Source column info
            
        Returns:
            Best target column or None
        """
        # Prefer primary keys
        primary_keys = [col for col in target_table.columns if col.is_primary_key]
        if primary_keys:
            return primary_keys[0]
        
        # Look for columns with matching data type and common names
        candidates = []
        for column in target_table.columns:
            if column.data_type == source_column.data_type:
                score = 0
                if column.name.lower() in ['id', 'key', 'pk']:
                    score += 10
                if column.mode == 'REQUIRED':
                    score += 5
                candidates.append((score, column))
        
        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            return candidates[0][1]
        
        return None
    
    def _is_potential_relationship(self, col1: ColumnInfo, col2: ColumnInfo) -> bool:
        """Check if two columns could be related.
        
        Args:
            col1: First column
            col2: Second column
            
        Returns:
            True if potentially related
        """
        # Must have same data type
        if col1.data_type != col2.data_type:
            return False
        
        # At least one should be required
        if col1.mode == "NULLABLE" and col2.mode == "NULLABLE":
            return False
        
        # Check naming similarity
        name1 = col1.name.lower()
        name2 = col2.name.lower()
        
        # Common relationship patterns
        patterns = [
            (r'(.+)_id$', r'\1_id$'),
            (r'(.+)_key$', r'\1_key$'),
            (r'^id$', r'(.+)_id$'),
            (r'^key$', r'(.+)_key$'),
        ]
        
        for pattern1, pattern2 in patterns:
            if (re.match(pattern1, name1) and re.match(pattern2, name2)) or \
               (re.match(pattern1, name2) and re.match(pattern2, name1)):
                return True
        
        return False
    
    def _determine_relationship_type(self, source_table: TableSchema, source_column: ColumnInfo,
                                   target_table: TableSchema, target_column: ColumnInfo) -> RelationshipType:
        """Determine the type of relationship between two columns.
        
        Args:
            source_table: Source table
            source_column: Source column
            target_table: Target table
            target_column: Target column
            
        Returns:
            Relationship type
        """
        # Simple heuristic: if target column is PK, it's many-to-one
        if target_column.is_primary_key:
            return RelationshipType.MANY_TO_ONE
        
        # If source column is PK, it's one-to-many
        if source_column.is_primary_key:
            return RelationshipType.ONE_TO_MANY
        
        # Default to many-to-one
        return RelationshipType.MANY_TO_ONE
    
    def _apply_naming_pattern(self, pattern_rule: NamingPattern, 
                            tables: List[TableSchema],
                            table_map: Dict[str, TableSchema]) -> List[Relationship]:
        """Apply a naming pattern rule.
        
        Args:
            pattern_rule: Naming pattern rule
            tables: List of table schemas
            table_map: Map of table_id to TableSchema
            
        Returns:
            List of relationships found by this pattern
        """
        relationships = []
        pattern = re.compile(pattern_rule.pattern, re.IGNORECASE)
        
        for table in tables:
            for column in table.columns:
                if pattern.match(column.name):
                    # Extract base name
                    match = pattern.match(column.name)
                    if match:
                        base_name = match.group(1) if match.groups() else column.name
                        target_table_name = base_name + pattern_rule.target_suffix
                        
                        if target_table_name in table_map:
                            target_table = table_map[target_table_name]
                            target_column = self._find_best_target_column(target_table, column)
                            if target_column:
                                relationship = Relationship(
                                    source_table=table.table_id,
                                    source_column=column.name,
                                    target_table=target_table.table_id,
                                    target_column=target_column.name,
                                    relationship_type=RelationshipType.MANY_TO_ONE,
                                    confidence=pattern_rule.confidence,
                                    detection_method="custom_naming_pattern"
                                )
                                relationships.append(relationship)
        
        return relationships
    
    def _find_column_by_name(self, table: TableSchema, column_name: str) -> Optional[ColumnInfo]:
        """Find column by name in table.
        
        Args:
            table: Table schema
            column_name: Column name to find
            
        Returns:
            Column info or None
        """
        for column in table.columns:
            if column.name == column_name:
                return column
        return None
    
    def _resolve_relationship_conflicts(self, relationships: List[Relationship]) -> List[Relationship]:
        """Resolve conflicts between relationships and remove duplicates.
        
        Args:
            relationships: List of all relationships
            
        Returns:
            List of unique, resolved relationships
        """
        # Group relationships by source and target
        relationship_map = {}
        
        for rel in relationships:
            key = (rel.source_table, rel.source_column, rel.target_table, rel.target_column)
            
            if key not in relationship_map:
                relationship_map[key] = rel
            else:
                # Keep the relationship with higher confidence
                existing = relationship_map[key]
                if rel.confidence > existing.confidence:
                    relationship_map[key] = rel
                elif rel.confidence == existing.confidence:
                    # Prefer custom rules over automatic detection
                    if rel.is_custom and not existing.is_custom:
                        relationship_map[key] = rel
        
        return list(relationship_map.values())
    
    def _filter_relationships(self, relationships: List[Relationship]) -> List[Relationship]:
        """Filter relationships to reduce clutter and show only meaningful connections.
        
        Args:
            relationships: List of all detected relationships
            
        Returns:
            Filtered list of meaningful relationships
        """
        if not relationships:
            return relationships
        
        # Group relationships by source table
        table_relationships = {}
        for rel in relationships:
            if rel.source_table not in table_relationships:
                table_relationships[rel.source_table] = []
            table_relationships[rel.source_table].append(rel)
        
        filtered = []
        
        for source_table, rels in table_relationships.items():
            # Sort by confidence (highest first)
            rels.sort(key=lambda x: x.confidence, reverse=True)
            
            # Keep only the top relationships per source table
            # Limit to 3-5 relationships per table to reduce clutter
            max_rels_per_table = min(5, len(rels))
            
            # Prefer relationships with higher confidence and better naming patterns
            meaningful_rels = []
            for rel in rels[:max_rels_per_table]:
                # Skip very low confidence relationships
                if rel.confidence < 0.2:  # Lowered threshold
                    continue
                
                # Prefer relationships that follow naming conventions
                if (rel.detection_method in ["enhanced_pk_fk", "foreign_key"] or
                    rel.confidence >= 0.5):  # Lowered threshold
                    meaningful_rels.append(rel)
            
            # If we don't have enough high-confidence relationships, 
            # include some medium-confidence ones
            if len(meaningful_rels) < 2:  # Lowered minimum
                for rel in rels:
                    if rel not in meaningful_rels and rel.confidence >= 0.3:  # Lowered threshold
                        meaningful_rels.append(rel)
                        if len(meaningful_rels) >= 2:  # Lowered minimum
                            break
            
            filtered.extend(meaningful_rels)
        
        # Remove duplicate relationships (same source->target pair)
        seen_pairs = set()
        final_filtered = []
        for rel in filtered:
            pair = (rel.source_table, rel.target_table)
            if pair not in seen_pairs:
                seen_pairs.add(pair)
                final_filtered.append(rel)
        
        logger.debug(f"Filtered {len(relationships)} relationships down to {len(final_filtered)} meaningful relationships")
        return final_filtered


class RelationshipValidator:
    """Validates detected relationships."""
    
    def __init__(self):
        """Initialize relationship validator."""
        pass
    
    def validate_relationships(self, relationships: List[Relationship], 
                            tables: List[TableSchema]) -> List[Relationship]:
        """Validate relationships against table schemas.
        
        Args:
            relationships: List of relationships to validate
            tables: List of table schemas
            
        Returns:
            List of valid relationships
        """
        table_map = {table.table_id: table for table in tables}
        valid_relationships = []
        
        for rel in relationships:
            if self._is_valid_relationship(rel, table_map):
                valid_relationships.append(rel)
            else:
                logger.warning(f"Invalid relationship: {rel.source_table}.{rel.source_column} -> {rel.target_table}.{rel.target_column}")
        
        return valid_relationships
    
    def _is_valid_relationship(self, relationship: Relationship, 
                             table_map: Dict[str, TableSchema]) -> bool:
        """Check if a relationship is valid.
        
        Args:
            relationship: Relationship to validate
            table_map: Map of table_id to TableSchema
            
        Returns:
            True if relationship is valid
        """
        # Check if source table exists
        if relationship.source_table not in table_map:
            return False
        
        # Check if target table exists
        if relationship.target_table not in table_map:
            return False
        
        source_table = table_map[relationship.source_table]
        target_table = table_map[relationship.target_table]
        
        # Check if source column exists
        source_column = self._find_column(source_table, relationship.source_column)
        if not source_column:
            return False
        
        # Check if target column exists
        target_column = self._find_column(target_table, relationship.target_column)
        if not target_column:
            return False
        
        # Check data type compatibility
        if not self._are_types_compatible(source_column.data_type, target_column.data_type):
            return False
        
        return True
    
    def _find_column(self, table: TableSchema, column_name: str) -> Optional[ColumnInfo]:
        """Find column in table.
        
        Args:
            table: Table schema
            column_name: Column name
            
        Returns:
            Column info or None
        """
        for column in table.columns:
            if column.name == column_name:
                return column
        return None
    
    def _are_types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two data types are compatible.
        
        Args:
            type1: First data type
            type2: Second data type
            
        Returns:
            True if types are compatible
        """
        # Exact match
        if type1 == type2:
            return True
        
        # Compatible type groups
        compatible_groups = [
            ["INTEGER", "INT64"],
            ["STRING", "TEXT"],
            ["FLOAT", "FLOAT64"],
            ["BOOLEAN", "BOOL"],
        ]
        
        for group in compatible_groups:
            if type1 in group and type2 in group:
                return True
        
        return False
