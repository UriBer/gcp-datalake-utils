"""Relationship cache system for managing cached relationship data."""

import json
import time
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging

from .models import Relationship

logger = logging.getLogger(__name__)


class RelationshipCache:
    """Manages cached relationship data for faster processing."""

    def __init__(self, cache_dir: str = ".cache"):
        """Initialize relationship cache.

        Args:
            cache_dir: Directory to store cache files.
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.memory_cache = {}
        self.cache_metadata = {}
        self.cache_ttl_hours = 24  # Default TTL in hours

    def get_cache_key(self, table1: str, table2: str) -> str:
        """Generate a cache key for two tables."""
        # Sort table names to ensure consistent key regardless of order
        sorted_tables = sorted([table1, table2])
        return f"{sorted_tables[0]}_{sorted_tables[1]}"

    def get_cached_relationship(self, table1: str, table2: str) -> Optional[Relationship]:
        """Get cached relationship if exists and still valid.

        Args:
            table1: First table name
            table2: Second table name

        Returns:
            Cached relationship if valid, None otherwise
        """
        cache_key = self.get_cache_key(table1, table2)

        # Check memory cache first
        if cache_key in self.memory_cache:
            logger.debug(f"Found relationship in memory cache: {cache_key}")
            return self.memory_cache[cache_key]

        # Check disk cache
        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists() and self._is_cache_valid(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                    relationship = Relationship(**data)
                    self.memory_cache[cache_key] = relationship
                    logger.debug(f"Loaded relationship from disk cache: {cache_key}")
                    return relationship
            except Exception as e:
                logger.warning(f"Error loading cached relationship {cache_key}: {e}")
                return None

        return None

    def cache_relationship(self, relationship: Relationship):
        """Cache a relationship for future use.

        Args:
            relationship: Relationship to cache
        """
        cache_key = self.get_cache_key(relationship.source_table, relationship.target_table)
        
        # Store in memory cache
        self.memory_cache[cache_key] = relationship

        # Persist to disk
        try:
            cache_file = self.cache_dir / f"{cache_key}.json"
            with open(cache_file, 'w') as f:
                json.dump(relationship.dict(), f, indent=2)
            
            # Update metadata
            self.cache_metadata[cache_key] = {
                "timestamp": time.time(),
                "source_table": relationship.source_table,
                "target_table": relationship.target_table,
                "confidence": relationship.confidence
            }
            
            logger.debug(f"Cached relationship: {cache_key}")
        except Exception as e:
            logger.error(f"Error caching relationship {cache_key}: {e}")

    def _is_cache_valid(self, cache_file: Path) -> bool:
        """Check if cache file is still valid based on TTL.

        Args:
            cache_file: Path to cache file

        Returns:
            True if cache is valid, False otherwise
        """
        try:
            file_mtime = cache_file.stat().st_mtime
            current_time = time.time()
            age_hours = (current_time - file_mtime) / 3600
            
            return age_hours < self.cache_ttl_hours
        except Exception:
            return False

    def clear_cache(self, table_pattern: Optional[str] = None):
        """Clear cache entries.

        Args:
            table_pattern: Optional pattern to match table names. If None, clears all.
        """
        if table_pattern:
            # Clear specific entries matching pattern
            keys_to_remove = []
            for key in self.memory_cache:
                if table_pattern in key:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self.memory_cache[key]
                cache_file = self.cache_dir / f"{key}.json"
                if cache_file.exists():
                    cache_file.unlink()
        else:
            # Clear all cache
            self.memory_cache.clear()
            for cache_file in self.cache_dir.glob("*.json"):
                cache_file.unlink()
        
        logger.info(f"Cleared cache for pattern: {table_pattern or 'all'}")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        memory_count = len(self.memory_cache)
        disk_count = len(list(self.cache_dir.glob("*.json")))
        
        return {
            "memory_cache_entries": memory_count,
            "disk_cache_entries": disk_count,
            "cache_dir": str(self.cache_dir),
            "cache_ttl_hours": self.cache_ttl_hours
        }
