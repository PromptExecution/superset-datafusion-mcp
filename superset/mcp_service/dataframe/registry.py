# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Virtual Dataset Registry

Provides an in-memory registry for DataFrame-based virtual datasets.
Virtual datasets allow AI agents to ingest DataFrame data via the MCP service
and use it for chart and dashboard generation without requiring database storage.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from superset.common.query_object import QueryObject

logger = logging.getLogger(__name__)

# Global registry instance
_registry: VirtualDatasetRegistry | None = None
_registry_lock = threading.Lock()


@dataclass
class VirtualDataset:
    """
    Represents a DataFrame-based virtual dataset.

    A virtual dataset is an in-memory Arrow table that can be queried
    using Superset's query interface without requiring database storage.
    Virtual datasets are session-scoped and automatically cleaned up
    based on their TTL (time-to-live).
    """

    id: str
    name: str
    schema: pa.Schema
    table: pa.Table
    created_at: datetime
    ttl: timedelta
    owner_session: str
    row_count: int = field(init=False)
    size_bytes: int = field(init=False)

    def __post_init__(self) -> None:
        """Calculate derived fields after initialization."""
        self.row_count = self.table.num_rows
        # Estimate size using Arrow's buffer sizes
        self.size_bytes = sum(
            buf.size for chunk in self.table.columns for buf in chunk.buffers() if buf
        )

    @property
    def is_expired(self) -> bool:
        """Check if this virtual dataset has expired based on TTL."""
        if self.ttl.total_seconds() <= 0:
            return False  # TTL of 0 means no expiration
        return datetime.now(timezone.utc) > self.created_at + self.ttl

    @property
    def expires_at(self) -> datetime | None:
        """Get the expiration time, or None if no expiration."""
        if self.ttl.total_seconds() <= 0:
            return None
        return self.created_at + self.ttl

    @property
    def column_names(self) -> list[str]:
        """Get list of column names."""
        return self.schema.names

    def get_column_info(self) -> list[dict[str, str | bool]]:
        """
        Get column metadata for the virtual dataset.

        Returns:
            List of column information dictionaries with name, type, and is_dttm.
        """
        columns = []
        for field_item in self.schema:
            columns.append(
                {
                    "column_name": field_item.name,
                    "type": str(field_item.type),
                    "is_dttm": pa.types.is_temporal(field_item.type),
                }
            )
        return columns


class VirtualDatasetRegistry:
    """
    In-memory registry for virtual datasets.

    This registry manages the lifecycle of virtual datasets, including:
    - Registration of new datasets from Arrow tables
    - Retrieval of datasets by ID
    - Automatic cleanup of expired datasets
    - Enforcement of size and count limits

    The registry is thread-safe and can be used from multiple concurrent
    MCP tool invocations.
    """

    def __init__(
        self,
        max_size_mb: int = 100,
        max_count: int = 10,
        default_ttl_minutes: int = 60,
    ):
        """
        Initialize the virtual dataset registry.

        Args:
            max_size_mb: Maximum size in MB for a single dataset
            max_count: Maximum number of datasets per session
            default_ttl_minutes: Default TTL in minutes for new datasets
        """
        self._datasets: dict[str, VirtualDataset] = {}
        self._lock = threading.Lock()
        self._max_size_bytes = max_size_mb * 1024 * 1024
        self._max_count = max_count
        self._default_ttl = timedelta(minutes=default_ttl_minutes)

    def register(
        self,
        name: str,
        table: pa.Table,
        session_id: str,
        ttl: timedelta | None = None,
    ) -> str:
        """
        Register a DataFrame as a virtual dataset.

        Args:
            name: Display name for the dataset
            table: Arrow Table containing the data
            session_id: MCP session ID that owns this dataset
            ttl: Optional time-to-live (uses default if not specified)

        Returns:
            Unique dataset ID (UUID)

        Raises:
            ValueError: If the dataset exceeds size limits or count limits
        """
        # Clean up expired datasets first
        self.cleanup_expired()

        # Validate size
        size_bytes = sum(
            buf.size for chunk in table.columns for buf in chunk.buffers() if buf
        )
        if size_bytes > self._max_size_bytes:
            raise ValueError(
                f"Dataset size ({size_bytes / 1024 / 1024:.2f} MB) exceeds "
                f"limit ({self._max_size_bytes / 1024 / 1024:.2f} MB)"
            )

        with self._lock:
            # Check count limit for this session
            session_count = sum(
                1 for ds in self._datasets.values() if ds.owner_session == session_id
            )
            if session_count >= self._max_count:
                raise ValueError(
                    f"Session has reached maximum dataset count ({self._max_count})"
                )

            # Generate unique ID
            dataset_id = str(uuid.uuid4())

            # Create virtual dataset
            dataset = VirtualDataset(
                id=dataset_id,
                name=name,
                schema=table.schema,
                table=table,
                created_at=datetime.now(timezone.utc),
                ttl=ttl if ttl is not None else self._default_ttl,
                owner_session=session_id,
            )

            self._datasets[dataset_id] = dataset

            logger.info(
                "Registered virtual dataset: id=%s, name=%s, rows=%d, size_mb=%.2f",
                dataset_id,
                name,
                dataset.row_count,
                dataset.size_bytes / 1024 / 1024,
            )

            return dataset_id

    def get(self, dataset_id: str) -> VirtualDataset | None:
        """
        Retrieve a virtual dataset by ID.

        Args:
            dataset_id: The unique dataset ID

        Returns:
            The VirtualDataset if found and not expired, None otherwise
        """
        with self._lock:
            dataset = self._datasets.get(dataset_id)

            if dataset is None:
                return None

            if dataset.is_expired:
                # Clean up expired dataset
                del self._datasets[dataset_id]
                logger.info("Virtual dataset expired: id=%s", dataset_id)
                return None

            return dataset

    def remove(self, dataset_id: str) -> bool:
        """
        Remove a virtual dataset from the registry.

        Args:
            dataset_id: The unique dataset ID

        Returns:
            True if the dataset was removed, False if not found
        """
        with self._lock:
            if dataset_id in self._datasets:
                del self._datasets[dataset_id]
                logger.info("Virtual dataset removed: id=%s", dataset_id)
                return True
            return False

    def list_datasets(
        self, session_id: str | None = None
    ) -> list[dict[str, str | int | datetime | None]]:
        """
        List all virtual datasets, optionally filtered by session.

        Args:
            session_id: Optional session ID to filter by

        Returns:
            List of dataset metadata dictionaries
        """
        # Clean up expired datasets first
        self.cleanup_expired()

        with self._lock:
            datasets = []
            for dataset in self._datasets.values():
                if session_id is None or dataset.owner_session == session_id:
                    datasets.append(
                        {
                            "id": dataset.id,
                            "name": dataset.name,
                            "row_count": dataset.row_count,
                            "size_bytes": dataset.size_bytes,
                            "created_at": dataset.created_at,
                            "expires_at": dataset.expires_at,
                            "column_count": len(dataset.column_names),
                        }
                    )
            return datasets

    def cleanup_expired(self) -> int:
        """
        Remove all expired datasets from the registry.

        Returns:
            Number of datasets removed
        """
        removed = 0
        with self._lock:
            expired_ids = [
                ds_id for ds_id, ds in self._datasets.items() if ds.is_expired
            ]
            for ds_id in expired_ids:
                del self._datasets[ds_id]
                removed += 1
                logger.info("Virtual dataset expired and removed: id=%s", ds_id)

        return removed

    def cleanup_session(self, session_id: str) -> int:
        """
        Remove all datasets owned by a specific session.

        Args:
            session_id: The session ID to clean up

        Returns:
            Number of datasets removed
        """
        removed = 0
        with self._lock:
            session_ids = [
                ds_id
                for ds_id, ds in self._datasets.items()
                if ds.owner_session == session_id
            ]
            for ds_id in session_ids:
                del self._datasets[ds_id]
                removed += 1
                logger.info("Virtual dataset removed for session cleanup: id=%s", ds_id)

        return removed

    @property
    def total_size_bytes(self) -> int:
        """Get total size of all datasets in bytes."""
        with self._lock:
            return sum(ds.size_bytes for ds in self._datasets.values())

    @property
    def total_count(self) -> int:
        """Get total number of datasets."""
        with self._lock:
            return len(self._datasets)


def get_registry() -> VirtualDatasetRegistry:
    """
    Get the global virtual dataset registry instance.

    The registry is created lazily on first access using configuration
    from the Flask app if available.

    Returns:
        The global VirtualDatasetRegistry instance
    """
    global _registry

    with _registry_lock:
        if _registry is None:
            # Try to get configuration from Flask app
            max_size_mb = 100
            max_count = 10
            default_ttl_minutes = 60

            try:
                from flask import current_app

                if current_app:
                    max_size_mb = current_app.config.get(
                        "MCP_VIRTUAL_DATASET_MAX_SIZE_MB", 100
                    )
                    max_count = current_app.config.get(
                        "MCP_VIRTUAL_DATASET_MAX_COUNT", 10
                    )
                    default_ttl_minutes = current_app.config.get(
                        "MCP_VIRTUAL_DATASET_DEFAULT_TTL_MINUTES", 60
                    )
            except RuntimeError:
                # Outside of Flask app context, use defaults
                pass

            _registry = VirtualDatasetRegistry(
                max_size_mb=max_size_mb,
                max_count=max_count,
                default_ttl_minutes=default_ttl_minutes,
            )

        return _registry


def reset_registry() -> None:
    """
    Reset the global registry (primarily for testing).

    This clears all registered datasets and creates a new registry instance.
    """
    global _registry
    with _registry_lock:
        _registry = None
