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

"""Tests for Pydantic schemas in DataFrame module."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from superset.mcp_service.dataframe.schemas import (
    ColumnSchema,
    DataFrameSourceCapability,
    DataFusionQueryResponse,
    DataFusionQueryRequest,
    DataFusionSourceConfig,
    IngestDataFrameRequest,
    IngestDataFrameResponse,
    ListSourceCapabilitiesRequest,
    ListSourceCapabilitiesResponse,
    PrometheusQueryRequest,
    VirtualDatasetInfo,
)


def test_column_schema_basic() -> None:
    """Test ColumnSchema with basic fields."""
    schema = ColumnSchema(name="test_column")
    assert schema.name == "test_column"
    assert schema.type is None
    assert schema.is_temporal is False
    assert schema.is_metric is False
    assert schema.label is None


def test_column_schema_full() -> None:
    """Test ColumnSchema with all fields."""
    schema = ColumnSchema(
        name="revenue",
        type="float64",
        is_temporal=False,
        is_metric=True,
        label="Total Revenue",
    )
    assert schema.name == "revenue"
    assert schema.type == "float64"
    assert schema.is_metric is True
    assert schema.label == "Total Revenue"


def test_ingest_dataframe_request_minimal() -> None:
    """Test IngestDataFrameRequest with minimal fields."""
    request = IngestDataFrameRequest(
        name="test_dataset",
        data="c29tZV9kYXRh",  # base64 encoded "some_data"
    )
    assert request.name == "test_dataset"
    assert request.data == "c29tZV9kYXRh"
    assert request.column_schema is None
    assert request.ttl_minutes == 60  # default


def test_ingest_dataframe_request_full() -> None:
    """Test IngestDataFrameRequest with all fields."""
    request = IngestDataFrameRequest(
        name="sales_data",
        data="c29tZV9kYXRh",
        column_schema=[
            ColumnSchema(name="date", is_temporal=True),
            ColumnSchema(name="amount", type="float64", is_metric=True),
        ],
        ttl_minutes=120,
        description="Monthly sales data",
    )
    assert request.name == "sales_data"
    assert request.column_schema is not None
    assert len(request.column_schema) == 2
    assert request.ttl_minutes == 120
    assert request.description == "Monthly sales data"


def test_ingest_dataframe_request_validation() -> None:
    """Test IngestDataFrameRequest validation."""
    # Empty name should fail
    with pytest.raises(ValidationError):
        IngestDataFrameRequest(name="", data="c29tZV9kYXRh")

    # TTL > 1440 should fail
    with pytest.raises(ValidationError):
        IngestDataFrameRequest(name="test", data="c29tZV9kYXRh", ttl_minutes=1441)

    # Negative TTL should fail
    with pytest.raises(ValidationError):
        IngestDataFrameRequest(name="test", data="c29tZV9kYXRh", ttl_minutes=-1)


def test_virtual_dataset_info() -> None:
    """Test VirtualDatasetInfo model."""
    now = datetime.now(timezone.utc)
    info = VirtualDatasetInfo(
        id="abc123",
        name="test_dataset",
        row_count=1000,
        column_count=5,
        size_bytes=10240,
        size_mb=0.01,
        created_at=now,
        expires_at=now,
        columns=[{"column_name": "id", "type": "int64", "is_dttm": False}],
    )
    assert info.id == "abc123"
    assert info.row_count == 1000
    assert info.column_count == 5


def test_ingest_dataframe_response_success() -> None:
    """Test successful IngestDataFrameResponse."""
    now = datetime.now(timezone.utc)
    response = IngestDataFrameResponse(
        success=True,
        dataset=VirtualDatasetInfo(
            id="abc123",
            name="test",
            row_count=100,
            column_count=3,
            size_bytes=1024,
            size_mb=0.001,
            created_at=now,
            expires_at=now,
            columns=[],
        ),
        dataset_id="abc123",
        usage_hint="Use 'virtual:abc123' with generate_chart",
    )
    assert response.success is True
    assert response.dataset_id == "abc123"
    assert response.virtual_dataset_id == "virtual:abc123"
    assert response.error is None


def test_ingest_dataframe_response_error() -> None:
    """Test error IngestDataFrameResponse."""
    response = IngestDataFrameResponse(
        success=False,
        error="Dataset size exceeds limit",
        error_code="SIZE_LIMIT_EXCEEDED",
    )
    assert response.success is False
    assert response.dataset is None
    assert response.error == "Dataset size exceeds limit"
    assert response.error_code == "SIZE_LIMIT_EXCEEDED"


def test_prometheus_query_request_defaults() -> None:
    """Test PrometheusQueryRequest with minimal valid fields."""
    request = PrometheusQueryRequest(
        base_url="http://prometheus:9090",
        promql="up",
    )
    assert request.query_type == "range"
    assert request.step_seconds == 60
    assert request.ingest_as_virtual_dataset is True


def test_datafusion_source_config_validation() -> None:
    """Test DataFusion source validation by source type."""
    parquet_source = DataFusionSourceConfig(
        name="metrics",
        source_type="parquet",
        path="/tmp/data.parquet",
    )
    assert parquet_source.path == "/tmp/data.parquet"

    with pytest.raises(ValidationError):
        DataFusionSourceConfig(name="bad", source_type="parquet")

    with pytest.raises(ValidationError):
        DataFusionSourceConfig(name="bad", source_type="arrow_ipc")

    with pytest.raises(ValidationError):
        DataFusionSourceConfig(name="bad", source_type="virtual_dataset")


def test_datafusion_query_request_validation() -> None:
    """Test DataFusion query request schema validation."""
    request = DataFusionQueryRequest(
        sql="SELECT * FROM metrics",
        sources=[
            DataFusionSourceConfig(
                name="metrics",
                source_type="parquet",
                path="/tmp/data.parquet",
            )
        ],
        ingest_result=True,
    )
    assert request.limit == 1000
    assert request.ingest_result is True

    with pytest.raises(ValidationError):
        DataFusionQueryRequest(sql="SELECT 1", sources=[])


def test_prometheus_query_request_time_window_validation() -> None:
    """Test range query time window validation."""
    now = datetime.now(timezone.utc)
    with pytest.raises(ValidationError):
        PrometheusQueryRequest(
            base_url="http://prometheus:9090",
            promql="up",
            query_type="range",
            start_time=now,
            end_time=now - timedelta(minutes=5),
        )


def test_datafusion_query_response_source_capabilities() -> None:
    """DataFusion responses accept source capability metadata."""
    response = DataFusionQueryResponse(
        success=True,
        rows=[{"value": 1}],
        columns=[{"name": "value", "type": "int64"}],
        row_count=1,
        source_capabilities=[
            DataFrameSourceCapability(
                source_type="parquet",
                adapter_name="ParquetSourceAdapter",
                supports_streaming=True,
                supports_projection_pushdown=True,
                supports_predicate_pushdown=True,
                supports_sql_pushdown=True,
                supports_virtual_dataset_ingestion=True,
            )
        ],
    )
    assert len(response.source_capabilities) == 1
    assert response.source_capabilities[0].source_type == "parquet"


def test_list_source_capabilities_schemas() -> None:
    """Capability list request/response schemas validate correctly."""
    request = ListSourceCapabilitiesRequest(
        source_types=["parquet", "virtual_dataset"],
        include_prometheus=False,
    )
    assert request.source_types == ["parquet", "virtual_dataset"]
    assert request.include_prometheus is False

    response = ListSourceCapabilitiesResponse(
        success=True,
        capabilities=[
            DataFrameSourceCapability(
                source_type="parquet",
                adapter_name="ParquetSourceAdapter",
                supports_streaming=True,
                supports_projection_pushdown=True,
                supports_predicate_pushdown=True,
                supports_sql_pushdown=True,
                supports_virtual_dataset_ingestion=True,
            )
        ],
        total_count=1,
    )
    assert response.success is True
    assert response.total_count == 1
