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
Pydantic schemas for DataFrame MCP tools.

Defines request and response models for DataFrame ingestion, virtual dataset
management, and related operations.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class ColumnSchema(BaseModel):
    """
    Column definition for DataFrame ingestion.

    Provides explicit schema information for a column, allowing users to
    override inferred types and specify semantic meaning.
    """

    name: str = Field(description="Column name in the DataFrame")
    type: str | None = Field(
        default=None,
        description=(
            "Arrow type string (e.g., 'int64', 'utf8', 'timestamp[ns]'). "
            "If not provided, type is inferred from data."
        ),
    )
    is_temporal: bool = Field(
        default=False,
        description="Whether this column should be treated as a datetime column",
    )
    is_metric: bool = Field(
        default=False,
        description="Whether this column can be used as a metric (for aggregations)",
    )
    label: str | None = Field(default=None, description="Display label for the column")


class IngestDataFrameRequest(BaseModel):
    """
    Request to ingest a DataFrame via MCP.

    Allows AI agents to upload DataFrame data directly without requiring
    database storage. The data is registered as a virtual dataset that
    can be used with chart generation tools.
    """

    name: str = Field(
        description="Display name for the virtual dataset",
        min_length=1,
        max_length=100,
    )
    data: str = Field(
        description=(
            "Base64-encoded Arrow IPC stream containing the DataFrame data. "
            "Generate this by serializing a PyArrow Table to IPC format."
        ),
    )
    column_schema: list[ColumnSchema] | None = Field(
        default=None,
        description=(
            "Optional explicit schema for the DataFrame columns. If not provided, "
            "schema is inferred from the Arrow data."
        ),
    )
    ttl_minutes: int = Field(
        default=60,
        ge=0,
        le=1440,  # Max 24 hours
        description=(
            "Time-to-live in minutes. Set to 0 for session lifetime "
            "(cleaned up when session ends). Default is 60 minutes."
        ),
    )
    allow_cross_session: bool = Field(
        default=False,
        description=(
            "Allow access to this virtual dataset from other MCP sessions "
            "belonging to the same user. Defaults to False."
        ),
    )
    description: str | None = Field(
        default=None,
        max_length=500,
        description="Optional description of the dataset",
    )


class VirtualDatasetInfo(BaseModel):
    """
    Information about a registered virtual dataset.

    Returned after successful dataset ingestion and when listing datasets.
    """

    id: str = Field(description="Unique dataset ID (UUID format)")
    name: str = Field(description="Display name")
    row_count: int = Field(description="Number of rows in the dataset")
    column_count: int = Field(description="Number of columns in the dataset")
    size_bytes: int = Field(description="Approximate size in bytes")
    size_mb: float = Field(description="Approximate size in megabytes")
    created_at: datetime = Field(description="When the dataset was created")
    expires_at: datetime | None = Field(description="When the dataset will expire")
    columns: list[dict[str, str | bool]] = Field(
        description="Column metadata including name, type, and is_dttm"
    )
    description: str | None = Field(default=None, description="Dataset description")


class IngestDataFrameResponse(BaseModel):
    """Response from DataFrame ingestion."""

    success: bool = Field(description="Whether ingestion was successful")
    dataset: VirtualDatasetInfo | None = Field(
        default=None, description="Virtual dataset info if successful"
    )
    dataset_id: str | None = Field(
        default=None,
        description="Raw dataset UUID",
    )
    virtual_dataset_id: str | None = Field(
        default=None,
        description=(
            "Prefixed dataset ID in 'virtual:{uuid}' format for virtual "
            "dataset integrations"
        ),
    )
    usage_hint: str | None = Field(
        default=None,
        description="Hint for how to use the virtual dataset with other tools",
    )
    error: str | None = Field(default=None, description="Error message if failed")
    error_code: str | None = Field(default=None, description="Error code if failed")

    @model_validator(mode="after")
    def derive_virtual_dataset_id(self) -> "IngestDataFrameResponse":
        """
        Populate virtual_dataset_id from dataset_id when omitted.

        This keeps response construction ergonomic while preserving explicit
        override behavior when callers set virtual_dataset_id directly.
        """
        if self.virtual_dataset_id is None and self.dataset_id:
            if self.dataset_id.startswith("virtual:"):
                self.virtual_dataset_id = self.dataset_id
            else:
                self.virtual_dataset_id = f"virtual:{self.dataset_id}"
        return self


class ListVirtualDatasetsResponse(BaseModel):
    """Response from listing virtual datasets."""

    datasets: list[VirtualDatasetInfo] = Field(
        description="List of virtual datasets in the current session"
    )
    total_count: int = Field(description="Total number of datasets")
    total_size_mb: float = Field(description="Total size of all datasets in megabytes")


class RemoveVirtualDatasetRequest(BaseModel):
    """Request to remove a virtual dataset."""

    dataset_id: str = Field(
        description="The virtual dataset ID to remove (raw UUID or virtual:{uuid})"
    )


class RemoveVirtualDatasetResponse(BaseModel):
    """Response from removing a virtual dataset."""

    success: bool = Field(description="Whether removal was successful")
    message: str = Field(description="Status message")


class QueryVirtualDatasetRequest(BaseModel):
    """
    Request to query a virtual dataset.

    Allows executing SQL queries against virtual datasets using DuckDB.
    """

    dataset_id: str = Field(
        description="The virtual dataset ID to query (raw UUID or virtual:{uuid})"
    )
    sql: str = Field(
        description=(
            "SQL query to execute. The virtual dataset is available "
            "as the 'data' table. Example: 'SELECT * FROM data LIMIT 10'"
        ),
        min_length=1,
    )
    limit: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Maximum number of rows to return",
    )


class QueryVirtualDatasetResponse(BaseModel):
    """Response from querying a virtual dataset."""

    success: bool = Field(description="Whether query was successful")
    rows: list[dict[str, str | int | float | bool | None]] | None = Field(
        default=None, description="Query result rows"
    )
    columns: list[dict[str, str]] | None = Field(
        default=None, description="Column metadata for results"
    )
    row_count: int | None = Field(default=None, description="Number of rows returned")
    error: str | None = Field(default=None, description="Error message if failed")


class PrometheusQueryRequest(BaseModel):
    """Request to query Prometheus and optionally register a virtual dataset."""

    base_url: str = Field(
        description="Prometheus base URL, e.g. http://prometheus:9090"
    )
    promql: str = Field(description="PromQL query expression", min_length=1)
    query_type: Literal["range", "instant"] = Field(
        default="range",
        description="Prometheus API mode: range or instant query",
    )
    start_time: datetime | None = Field(
        default=None,
        description="Range query start time (UTC if naive); defaults to 1 hour ago",
    )
    end_time: datetime | None = Field(
        default=None,
        description="Range query end time (UTC if naive); defaults to now",
    )
    step_seconds: int = Field(
        default=60,
        ge=1,
        le=86400,
        description="Range query step size in seconds",
    )
    timeout_seconds: int = Field(
        default=30,
        ge=1,
        le=300,
        description="HTTP timeout for Prometheus request",
    )
    verify_ssl: bool = Field(default=True, description="Verify TLS certificates")
    ingest_as_virtual_dataset: bool = Field(
        default=True,
        description="Register query results as an MCP virtual dataset",
    )
    dataset_name: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
        description="Optional name override for ingested virtual dataset",
    )
    ttl_minutes: int = Field(
        default=60,
        ge=0,
        le=1440,
        description="TTL for ingested virtual dataset when ingestion is enabled",
    )
    allow_cross_session: bool = Field(
        default=False,
        description="Allow same-user cross-session access to ingested dataset",
    )

    @model_validator(mode="after")
    def validate_temporal_window(self) -> "PrometheusQueryRequest":
        """Validate time-range constraints for range queries."""
        if self.query_type == "range":
            if self.start_time and self.end_time and self.start_time > self.end_time:
                raise ValueError("start_time must be less than or equal to end_time")
        return self


class PrometheusQueryResponse(BaseModel):
    """Response from querying Prometheus."""

    success: bool = Field(description="Whether query execution succeeded")
    result_type: str | None = Field(
        default=None,
        description="Prometheus result type, e.g. matrix or vector",
    )
    rows: list[dict[str, str | int | float | bool | None]] | None = Field(
        default=None,
        description="Flattened Prometheus result rows",
    )
    columns: list[dict[str, str]] | None = Field(
        default=None,
        description="Column metadata for flattened rows",
    )
    row_count: int | None = Field(default=None, description="Number of rows returned")
    dataset: VirtualDatasetInfo | None = Field(
        default=None,
        description="Virtual dataset info when ingestion is enabled",
    )
    dataset_id: str | None = Field(
        default=None,
        description="Raw virtual dataset UUID when ingestion is enabled",
    )
    virtual_dataset_id: str | None = Field(
        default=None,
        description="Prefixed virtual dataset ID when ingestion is enabled",
    )
    source_capabilities: list["DataFrameSourceCapability"] = Field(
        default_factory=list,
        description="Capabilities for source adapters used during execution",
    )
    warning: str | None = Field(
        default=None,
        description="Non-fatal warning message",
    )
    error: str | None = Field(default=None, description="Error message if failed")
    error_code: str | None = Field(default=None, description="Error code if failed")


class DataFusionSourceConfig(BaseModel):
    """Source registration entry for DataFusion query execution."""

    name: str = Field(
        description="SQL table name to register in DataFusion",
        min_length=1,
    )
    source_type: Literal["parquet", "arrow_ipc", "virtual_dataset"] = Field(
        description="Data source type to register"
    )
    path: str | None = Field(
        default=None,
        description="Filesystem path or URI for parquet source_type",
    )
    data: str | None = Field(
        default=None,
        description="Base64 Arrow IPC payload for arrow_ipc source_type",
    )
    dataset_id: str | None = Field(
        default=None,
        description="Virtual dataset ID for virtual_dataset source_type",
    )

    @model_validator(mode="after")
    def validate_required_source_fields(self) -> "DataFusionSourceConfig":
        """Validate per-source required fields by source_type."""
        if self.source_type == "parquet" and not self.path:
            raise ValueError("path is required when source_type='parquet'")
        if self.source_type == "arrow_ipc" and not self.data:
            raise ValueError("data is required when source_type='arrow_ipc'")
        if self.source_type == "virtual_dataset" and not self.dataset_id:
            raise ValueError(
                "dataset_id is required when source_type='virtual_dataset'"
            )
        return self


class DataFrameSourceCapability(BaseModel):
    """Capability metadata for dataframe source adapters."""

    source_type: str = Field(description="Source type identifier")
    adapter_name: str = Field(description="Adapter class name")
    supports_streaming: bool = Field(
        description="Whether source supports streaming ingestion/read patterns"
    )
    supports_projection_pushdown: bool = Field(
        description="Whether source supports column projection pushdown"
    )
    supports_predicate_pushdown: bool = Field(
        description="Whether source supports predicate/filter pushdown"
    )
    supports_sql_pushdown: bool = Field(
        description="Whether source can execute SQL in source-native runtime"
    )
    supports_virtual_dataset_ingestion: bool = Field(
        description="Whether source output can be ingested as MCP virtual datasets"
    )


class ListSourceCapabilitiesRequest(BaseModel):
    """Request to list dataframe source adapter capabilities."""

    source_types: list[str] | None = Field(
        default=None,
        description=(
            "Optional source types to include. Defaults to all known source types."
        ),
    )
    include_prometheus: bool = Field(
        default=True,
        description="Include Prometheus HTTP source capability metadata",
    )


class ListSourceCapabilitiesResponse(BaseModel):
    """Response listing dataframe source capability metadata."""

    success: bool = Field(description="Whether capability discovery succeeded")
    capabilities: list[DataFrameSourceCapability] = Field(
        default_factory=list,
        description="Capability metadata entries",
    )
    total_count: int = Field(description="Number of returned capability entries")
    error: str | None = Field(default=None, description="Error message if failed")


class DataFusionQueryRequest(BaseModel):
    """Request to execute SQL via DataFusion against Parquet/Arrow sources."""

    sql: str = Field(description="SQL query to execute via DataFusion", min_length=1)
    sources: list[DataFusionSourceConfig] = Field(
        min_length=1,
        description="One or more source registrations for the DataFusion context",
    )
    limit: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Maximum number of rows to return",
    )
    ingest_result: bool = Field(
        default=False,
        description="Register query results as a virtual dataset",
    )
    result_dataset_name: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
        description="Optional name override for ingested query results",
    )
    ttl_minutes: int = Field(
        default=60,
        ge=0,
        le=1440,
        description="TTL for ingested result dataset when ingest_result is enabled",
    )
    allow_cross_session: bool = Field(
        default=False,
        description="Allow same-user cross-session access to ingested result dataset",
    )


class DataFusionQueryResponse(BaseModel):
    """Response from executing a DataFusion query."""

    success: bool = Field(description="Whether query execution succeeded")
    rows: list[dict[str, str | int | float | bool | None]] | None = Field(
        default=None,
        description="Result rows from DataFusion query",
    )
    columns: list[dict[str, str]] | None = Field(
        default=None,
        description="Column metadata for query results",
    )
    row_count: int | None = Field(default=None, description="Number of rows returned")
    dataset: VirtualDatasetInfo | None = Field(
        default=None,
        description="Virtual dataset info when ingest_result is enabled",
    )
    dataset_id: str | None = Field(
        default=None,
        description="Raw virtual dataset UUID when ingest_result is enabled",
    )
    virtual_dataset_id: str | None = Field(
        default=None,
        description="Prefixed virtual dataset ID when ingest_result is enabled",
    )
    source_capabilities: list[DataFrameSourceCapability] = Field(
        default_factory=list,
        description="Capabilities for source adapters used during execution",
    )
    warning: str | None = Field(
        default=None,
        description="Non-fatal warning message",
    )
    error: str | None = Field(default=None, description="Error message if failed")
    error_code: str | None = Field(default=None, description="Error code if failed")


class DataFrameAnalysisResult(BaseModel):
    """
    Analysis results for a DataFrame.

    Provides insights into the data that can help with chart recommendations.
    """

    temporal_columns: list[str] = Field(
        default_factory=list, description="Columns containing datetime values"
    )
    numeric_columns: list[str] = Field(
        default_factory=list, description="Columns containing numeric values"
    )
    categorical_columns: list[str] = Field(
        default_factory=list, description="Columns with low cardinality (categorical)"
    )
    high_cardinality_columns: list[str] = Field(
        default_factory=list, description="Columns with high cardinality"
    )
    suggested_chart_types: list[str] = Field(
        default_factory=list,
        description="Suggested chart types based on data characteristics",
    )
    data_quality_notes: list[str] = Field(
        default_factory=list, description="Notes about data quality issues"
    )


class ChartRecommendation(BaseModel):
    """A recommended chart configuration based on data analysis."""

    chart_type: Literal["line", "bar", "scatter", "pie", "table", "area"] = Field(
        description="Recommended chart type"
    )
    title: str = Field(description="Suggested chart title")
    x_column: str | None = Field(default=None, description="Suggested X-axis column")
    y_columns: list[str] = Field(
        default_factory=list, description="Suggested Y-axis columns"
    )
    group_by: str | None = Field(default=None, description="Suggested grouping column")
    reasoning: str = Field(description="Why this chart type is recommended")
    confidence: float = Field(
        ge=0.0, le=1.0, description="Confidence score for this recommendation"
    )


class AnalyzeDataFrameRequest(BaseModel):
    """Request to analyze a virtual dataset for chart recommendations."""

    dataset_id: str = Field(description="The virtual dataset ID to analyze")
    max_recommendations: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum number of chart recommendations to return",
    )


class AnalyzeDataFrameResponse(BaseModel):
    """Response from analyzing a virtual dataset."""

    success: bool = Field(description="Whether analysis was successful")
    analysis: DataFrameAnalysisResult | None = Field(
        default=None, description="Data analysis results"
    )
    recommendations: list[ChartRecommendation] = Field(
        default_factory=list, description="Chart recommendations"
    )
    error: str | None = Field(default=None, description="Error message if failed")


PrometheusQueryResponse.model_rebuild()
