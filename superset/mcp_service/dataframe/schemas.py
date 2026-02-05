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

from pydantic import BaseModel, Field


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
    label: str | None = Field(
        default=None, description="Display label for the column"
    )


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
        description=(
            "Dataset ID for use with generate_chart. "
            "Use as 'virtual:{dataset_id}' format."
        ),
    )
    usage_hint: str | None = Field(
        default=None,
        description="Hint for how to use the virtual dataset with other tools",
    )
    error: str | None = Field(default=None, description="Error message if failed")
    error_code: str | None = Field(default=None, description="Error code if failed")


class ListVirtualDatasetsResponse(BaseModel):
    """Response from listing virtual datasets."""

    datasets: list[VirtualDatasetInfo] = Field(
        description="List of virtual datasets in the current session"
    )
    total_count: int = Field(description="Total number of datasets")
    total_size_mb: float = Field(
        description="Total size of all datasets in megabytes"
    )


class RemoveVirtualDatasetRequest(BaseModel):
    """Request to remove a virtual dataset."""

    dataset_id: str = Field(description="The virtual dataset ID to remove")


class RemoveVirtualDatasetResponse(BaseModel):
    """Response from removing a virtual dataset."""

    success: bool = Field(description="Whether removal was successful")
    message: str = Field(description="Status message")


class QueryVirtualDatasetRequest(BaseModel):
    """
    Request to query a virtual dataset.

    Allows executing SQL queries against virtual datasets using DuckDB.
    """

    dataset_id: str = Field(description="The virtual dataset ID to query")
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
    row_count: int | None = Field(
        default=None, description="Number of rows returned"
    )
    error: str | None = Field(default=None, description="Error message if failed")


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
    group_by: str | None = Field(
        default=None, description="Suggested grouping column"
    )
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
