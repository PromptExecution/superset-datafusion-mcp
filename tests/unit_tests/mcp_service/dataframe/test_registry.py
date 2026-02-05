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

"""Tests for Virtual Dataset Registry."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from superset.mcp_service.dataframe.registry import (
    VirtualDataset,
    VirtualDatasetRegistry,
    reset_registry,
)


@pytest.fixture
def registry() -> VirtualDatasetRegistry:
    """Create a test registry with reasonable limits."""
    return VirtualDatasetRegistry(
        max_size_mb=10,
        max_count=5,
        default_ttl_minutes=30,
    )


@pytest.fixture
def sample_table() -> pa.Table:
    """Create a sample Arrow table for testing."""
    return pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "value": [10.5, 20.3, 30.1, 40.7, 50.9],
            "is_active": [True, False, True, True, False],
        }
    )


@pytest.fixture(autouse=True)
def reset_global_registry() -> None:
    """Reset the global registry before each test."""
    reset_registry()


def test_virtual_dataset_creation(sample_table: pa.Table) -> None:
    """Test VirtualDataset initialization."""
    dataset = VirtualDataset(
        id="test-id",
        name="test_dataset",
        schema=sample_table.schema,
        table=sample_table,
        created_at=datetime.now(timezone.utc),
        ttl=timedelta(minutes=30),
        owner_session="session-1",
    )

    assert dataset.id == "test-id"
    assert dataset.name == "test_dataset"
    assert dataset.row_count == 5
    assert dataset.size_bytes > 0
    assert dataset.column_names == ["id", "name", "value", "is_active"]
    assert not dataset.is_expired


def test_virtual_dataset_expiration() -> None:
    """Test VirtualDataset expiration logic."""
    table = pa.table({"x": [1, 2, 3]})

    # Create dataset that's already expired
    past_time = datetime.now(timezone.utc) - timedelta(hours=1)
    expired_dataset = VirtualDataset(
        id="expired",
        name="expired_dataset",
        schema=table.schema,
        table=table,
        created_at=past_time,
        ttl=timedelta(minutes=30),
        owner_session="session-1",
    )
    assert expired_dataset.is_expired

    # Create dataset with TTL of 0 (no expiration)
    no_expiry_dataset = VirtualDataset(
        id="no-expiry",
        name="no_expiry_dataset",
        schema=table.schema,
        table=table,
        created_at=past_time,
        ttl=timedelta(seconds=0),
        owner_session="session-1",
    )
    assert not no_expiry_dataset.is_expired
    assert no_expiry_dataset.expires_at is None


def test_registry_register_and_get(
    registry: VirtualDatasetRegistry, sample_table: pa.Table
) -> None:
    """Test registering and retrieving datasets."""
    dataset_id = registry.register(
        name="test_dataset",
        table=sample_table,
        session_id="session-1",
    )

    assert dataset_id is not None

    # Retrieve the dataset
    dataset = registry.get(dataset_id)
    assert dataset is not None
    assert dataset.name == "test_dataset"
    assert dataset.row_count == 5


def test_registry_remove(
    registry: VirtualDatasetRegistry, sample_table: pa.Table
) -> None:
    """Test removing datasets from registry."""
    dataset_id = registry.register(
        name="test_dataset",
        table=sample_table,
        session_id="session-1",
    )

    # Remove the dataset
    removed = registry.remove(dataset_id)
    assert removed is True

    # Verify it's gone
    assert registry.get(dataset_id) is None

    # Try to remove non-existent dataset
    removed_again = registry.remove(dataset_id)
    assert removed_again is False


def test_registry_size_limit(registry: VirtualDatasetRegistry) -> None:
    """Test that registry enforces size limits."""
    # Create a large table (larger than 10MB limit)
    large_data = {"col": list(range(2_000_000))}  # ~16MB
    large_table = pa.table(large_data)

    with pytest.raises(ValueError, match="exceeds limit"):
        registry.register(
            name="large_dataset",
            table=large_table,
            session_id="session-1",
        )


def test_registry_count_limit(
    registry: VirtualDatasetRegistry, sample_table: pa.Table
) -> None:
    """Test that registry enforces count limits."""
    # Register max_count datasets
    for i in range(5):
        registry.register(
            name=f"dataset_{i}",
            table=sample_table,
            session_id="session-1",
        )

    # Try to register one more
    with pytest.raises(ValueError, match="maximum dataset count"):
        registry.register(
            name="dataset_6",
            table=sample_table,
            session_id="session-1",
        )


def test_registry_list_datasets(
    registry: VirtualDatasetRegistry, sample_table: pa.Table
) -> None:
    """Test listing datasets."""
    # Register some datasets
    registry.register(name="ds1", table=sample_table, session_id="session-1")
    registry.register(name="ds2", table=sample_table, session_id="session-1")
    registry.register(name="ds3", table=sample_table, session_id="session-2")

    # List all datasets
    all_datasets = registry.list_datasets()
    assert len(all_datasets) == 3

    # List datasets for session-1
    session1_datasets = registry.list_datasets(session_id="session-1")
    assert len(session1_datasets) == 2

    # List datasets for session-2
    session2_datasets = registry.list_datasets(session_id="session-2")
    assert len(session2_datasets) == 1


def test_registry_cleanup_expired(
    registry: VirtualDatasetRegistry, sample_table: pa.Table
) -> None:
    """Test cleanup of expired datasets."""
    # Register with very short TTL
    dataset_id = registry.register(
        name="short_lived",
        table=sample_table,
        session_id="session-1",
        ttl=timedelta(seconds=0),  # 0 means no expiration
    )

    # Should still exist
    assert registry.get(dataset_id) is not None

    # Manually expire the dataset by modifying its created_at
    dataset = registry._datasets[dataset_id]
    dataset.created_at = datetime.now(timezone.utc) - timedelta(hours=2)
    dataset.ttl = timedelta(minutes=30)  # Set TTL so it's now expired

    # Cleanup should remove it
    removed = registry.cleanup_expired()
    assert removed == 1

    # Should be gone
    assert registry.get(dataset_id) is None


def test_registry_cleanup_session(
    registry: VirtualDatasetRegistry, sample_table: pa.Table
) -> None:
    """Test cleanup of all datasets for a session."""
    # Register datasets for different sessions
    registry.register(name="ds1", table=sample_table, session_id="session-1")
    registry.register(name="ds2", table=sample_table, session_id="session-1")
    registry.register(name="ds3", table=sample_table, session_id="session-2")

    # Cleanup session-1
    removed = registry.cleanup_session("session-1")
    assert removed == 2

    # Verify only session-2 remains
    all_datasets = registry.list_datasets()
    assert len(all_datasets) == 1
    assert all_datasets[0]["name"] == "ds3"


def test_virtual_dataset_get_column_info(sample_table: pa.Table) -> None:
    """Test getting column info from virtual dataset."""
    dataset = VirtualDataset(
        id="test-id",
        name="test_dataset",
        schema=sample_table.schema,
        table=sample_table,
        created_at=datetime.now(timezone.utc),
        ttl=timedelta(minutes=30),
        owner_session="session-1",
    )

    columns = dataset.get_column_info()
    assert len(columns) == 4

    # Check column types
    id_col = next(c for c in columns if c["column_name"] == "id")
    assert id_col["type"] == "int64"
    assert id_col["is_dttm"] is False

    name_col = next(c for c in columns if c["column_name"] == "name")
    assert "string" in name_col["type"].lower() or "utf8" in name_col["type"].lower()


def test_registry_total_size_and_count(
    registry: VirtualDatasetRegistry, sample_table: pa.Table
) -> None:
    """Test total size and count tracking."""
    # Initially empty
    assert registry.total_count == 0
    assert registry.total_size_bytes == 0

    # Add datasets
    registry.register(name="ds1", table=sample_table, session_id="session-1")
    registry.register(name="ds2", table=sample_table, session_id="session-1")

    assert registry.total_count == 2
    assert registry.total_size_bytes > 0
