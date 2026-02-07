<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
---
title: DataFrame Subsystem Maturity Report
hide_title: true
sidebar_position: 20
version: 1
---

# Apache Superset DataFrame Subsystem Maturity Report

This page has moved. Please refer to the canonical version of this document in the Developer Portal:

- [DataFrame Subsystem Maturity Report](developer_portal/dataframe-subsystem-maturity-report.md)

This stub is kept only to preserve existing links and avoid duplicated documentation content.

<!-- The detailed content has been consolidated into the Developer Portal version
     at docs/developer_portal/dataframe-subsystem-maturity-report.md to prevent
     divergence and confusion. -->
### 1.1 Core Implementation: `SupersetResultSet`

The `SupersetResultSet` class (located at `superset/result_set.py`) is the primary Arrow integration point in Superset:

```python
class SupersetResultSet:
    def __init__(
        self,
        data: DbapiResult,
        cursor_description: DbapiDescription,
        db_engine_spec: type[BaseEngineSpec],
    ):
        # Converts database results to PyArrow Table
        self.table = pa.Table.from_arrays(pa_data, names=column_names)
```

Key capabilities:

- Converts raw DBAPI results to `pa.Table` with automatic type inference.
- Handles edge cases: nested types, temporal types with timezone, large integers.
- Provides `pa_table` property for direct Arrow table access.
- Converts to pandas via `to_pandas_df()` with nullable integer support.

### 1.2 Type Mapping

The system maps PyArrow types to Superset's generic types:

```python
@staticmethod
def convert_pa_dtype(pa_dtype: pa.DataType) -> Optional[str]:
    if pa.types.is_boolean(pa_dtype): return "BOOL"
    if pa.types.is_integer(pa_dtype): return "INT"
    if pa.types.is_floating(pa_dtype): return "FLOAT"
    if pa.types.is_string(pa_dtype): return "STRING"
    if pa.types.is_temporal(pa_dtype): return "DATETIME"
    return None
```

### 1.3 Arrow IPC Support

Arrow IPC (Inter-Process Communication) is used for efficient result serialization in SQL Lab:

```python
# superset/sqllab/utils.py
def write_ipc_buffer(table: pa.Table) -> pa.Buffer:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()
```

This enables:

- Zero-copy deserialization of cached query results.
- Efficient transfer of large datasets.
- Type-safe serialization with full schema preservation.

### 1.4 CSV Upload with PyArrow Engine

The file upload system can use PyArrow for CSV parsing:

```python
# superset/commands/database/uploaders/csv_reader.py
def _get_csv_engine() -> Literal["c", "pyarrow"]:
    # Uses pyarrow engine when available for faster parsing
    pyarrow_spec = util.find_spec("pyarrow")
    if pyarrow_spec:
        return "pyarrow"
    return "c"
```

## 2. Pandas DataFrame Processing Pipeline

### 2.1 Post-Processing Functions

Superset provides a rich set of DataFrame transformations in `superset/utils/pandas_postprocessing/`:

| Function | Description |
| --- | --- |
| `aggregate` | Apply aggregation functions |
| `pivot` | Reshape data with pivot operations |
| `rolling` | Rolling window calculations |
| `resample` | Time-series resampling |
| `contribution` | Calculate percentage contributions |
| `compare` | Period-over-period comparisons |
| `boxplot` | Statistical boxplot calculations |
| `histogram` | Histogram bin calculations |
| `prophet` | Time-series forecasting (optional) |

### 2.2 Query Result Processing

The `QueryContextProcessor` manages DataFrame handling for chart rendering:

```python
# superset/common/query_context_processor.py
class QueryContextProcessor:
    cache_type: ClassVar[str] = "df"  # Caches pandas DataFrames

    def get_df_payload(self, query_obj: QueryObject) -> dict[str, Any]:
        # Returns cached or fresh DataFrame results
```

### 2.3 DataFrame Utilities

Additional utilities in `superset/common/utils/dataframe_utils.py`:

- `left_join_df`: DataFrame join operations.
- `full_outer_join_df`: Full outer joins.
- `df_metrics_to_num`: Metric type coercion.
- `is_datetime_series`: Type detection.

## 3. Obstacles to DataFrame Ingestion

### 3.1 Current Data Flow Architecture

```text
┌─────────────────┐    ┌──────────────┐    ┌────────────────┐
│  Database       │    │ SupersetRS   │    │ Pandas DF      │
│  (SQL Query)    │───▶│ (PyArrow)    │───▶│ (Processing)   │
└─────────────────┘    └──────────────┘    └────────────────┘
                                                    │
                                                    ▼
                                           ┌────────────────┐
                                           │ Chart/Dashboard│
                                           │ (JSON Response)│
                                           └────────────────┘
```

Problem: There is no direct path from external DataFrames to the visualization layer without going through a database.

### 3.2 Identified Obstacles

**Obstacle 1: Database-Centric Design**

The Explorable protocol (data source interface) assumes SQL-based querying:

```python
# superset/explorables/base.py
class Explorable(Protocol):
    def get_query_result(self, query_object: QueryObject) -> QueryResult:
        """Execute a query and return results."""

    def get_query_str(self, query_obj: QueryObjectDict) -> str:
        """Get the query string without executing."""
```

Impact: Cannot directly query DataFrame objects without SQL translation.

**Obstacle 2: Dataset Registration Requirement**

All data sources must be registered as "Datasets" with:

- Database connection.
- Table/view reference.
- Column metadata in database.

Impact: External DataFrames require upload to a database first.

**Obstacle 3: Security Model Assumptions**

Row-Level Security (RLS) and permissions are tied to database objects:

```python
@property
def perm(self) -> str:
    """Permission string for this explorable."""
    # Format: "[database].[schema].[table]"
```

Impact: Permission model doesn't accommodate in-memory data.

**Obstacle 4: Caching Layer Design**

The cache layer assumes cacheable, reproducible SQL queries:

```python
def query_cache_key(self, query_obj: QueryObject) -> str:
    # Cache key based on SQL query hash
```

Impact: In-memory DataFrames don't have stable cache keys.

### 3.3 Ergonomic Improvements Needed

| Area | Current State | Needed Improvement |
| --- | --- | --- |
| Data Source Protocol | SQL-only | Add DataFrame adapter |
| Dataset Model | Database-bound | Support virtual datasets |
| Column Metadata | From database schema | Allow explicit schema definition |
| Query Execution | SQL engine required | Allow DataFrame operations |
| Cache Keys | SQL-based | Support content-addressable hashing |
| Permissions | Database-centric | Add dataset-level permissions |

## 4. MCP Service Architecture Review

### 4.1 Current MCP Capabilities

The existing MCP service (`superset/mcp_service/`) provides:

- Tools: `execute_sql`, `generate_chart`, `generate_dashboard`, `list_datasets`, etc.
- Resources: Instance metadata, schema discovery.
- Prompts: Guided workflows.
- Auth: JWT and development mode authentication.
- Caching: Redis-backed for multi-pod deployments.

### 4.2 Current Dashboard Creation Flow

```text
┌─────────────┐    ┌──────────────┐    ┌────────────────┐
│ MCP Client  │    │ execute_sql  │    │ Database       │
│ (LLM Agent) │───▶│ Tool         │───▶│ Query          │
└─────────────┘    └──────────────┘    └────────────────┘
        │                                       │
        │          ┌──────────────┐             │
        └─────────▶│generate_chart│◀────────────┘
                   │ Tool         │      (via dataset_id)
                   └──────────────┘
                          │
                          ▼
                   ┌──────────────────┐
                   │ generate_dashboard│
                   │ Tool             │
                   └──────────────────┘
```

### 4.3 Gap Analysis

| Current Capability | Gap for DataFrame Ingestion |
| --- | --- |
| `execute_sql` returns DataFrame | No way to register as reusable dataset |
| `generate_chart` needs `dataset_id` | No dataset_id for ephemeral data |
| `list_datasets` shows DB tables | No ephemeral dataset discovery |
| Schema validation | No schema definition from DataFrame |

## 5. Plan: Fast MCP DataFrame Interface for Dashboards

### 5.1 Proposed Architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│                        MCP DataFrame Interface                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐    ┌────────────────┐    ┌────────────────┐   │
│  │ DataFrame    │    │ Virtual Dataset │    │ Chart Builder  │   │
│  │ Ingestion    │───▶│ Registry       │───▶│ (Temporary)    │   │
│  └──────────────┘    └────────────────┘    └────────────────┘   │
│         │                    │                     │             │
│         │ Arrow IPC          │ Schema              │ Config      │
│         ▼                    ▼                     ▼             │
│  ┌──────────────┐    ┌────────────────┐    ┌────────────────┐   │
│  │ Arrow Table  │    │ Column Metadata│    │ Superset Chart │   │
│  └──────────────┘    └────────────────┘    └────────────────┘   │
│                                                     │             │
│                                                     ▼             │
│                              ┌────────────────────────────────┐  │
│                              │     Dashboard Generator        │  │
│                              │     (Ephemeral/Persistent)     │  │
│                              └────────────────────────────────┘  │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 Implementation Plan

**Phase 1: Virtual Dataset Registry (Week 1-2)**

Objective: Create an in-memory registry for DataFrame-based datasets.

```python
# Proposed: superset/mcp_service/dataframe/registry.py
from dataclasses import dataclass
from typing import Dict, List
import pyarrow as pa
from datetime import datetime, timedelta

@dataclass
class VirtualDataset:
    """Represents a DataFrame-based virtual dataset."""
    id: str  # UUID for the virtual dataset
    name: str
    schema: pa.Schema
    table: pa.Table
    created_at: datetime
    ttl: timedelta  # Time-to-live for cleanup
    owner_session: str  # MCP session that created it

class VirtualDatasetRegistry:
    """In-memory registry for virtual datasets."""

    def register(self, name: str, table: pa.Table, ttl: timedelta) -> str:
        """Register a DataFrame as a virtual dataset."""

    def get(self, dataset_id: str) -> VirtualDataset | None:
        """Retrieve a virtual dataset."""

    def query(self, dataset_id: str, query_obj: QueryObject) -> pa.Table:
        """Execute a query against a virtual dataset using DuckDB."""

    def cleanup_expired(self) -> int:
        """Remove expired datasets."""
```

Key features:

- Session-scoped datasets (cleaned up when session ends).
- Optional persistence for sharing across sessions.
- DuckDB integration for SQL queries against Arrow tables.

**Phase 2: DataFrame Ingestion MCP Tool (Week 2-3)**

Objective: Create MCP tools for DataFrame ingestion.

```python
# Proposed: superset/mcp_service/dataframe/tool/ingest_dataframe.py
from pydantic import BaseModel, Field
from typing import List, Optional
import base64

class ColumnSchema(BaseModel):
    """Column definition for DataFrame ingestion."""
    name: str
    type: str  # Arrow type string
    is_temporal: bool = False
    is_metric: bool = False

class IngestDataFrameRequest(BaseModel):
    """Request to ingest a DataFrame via MCP."""
    name: str = Field(description="Display name for the dataset")
    data: str = Field(description="Base64-encoded Arrow IPC stream")
    schema: Optional[List[ColumnSchema]] = Field(
        default=None,
        description="Optional explicit schema (inferred if not provided)")
    ttl_minutes: int = Field(
        default=60,
        description="Time-to-live in minutes (0 for session lifetime)")

@tool(tags=["dataframe", "mutate"])
async def ingest_dataframe(
    request: IngestDataFrameRequest,
    ctx: Context
) -> IngestDataFrameResponse:
    """
    Ingest a DataFrame from Arrow IPC format for visualization.

    This tool allows AI agents to upload DataFrame data directly
    without requiring database storage. The data is registered as
    a virtual dataset that can be used with generate_chart.

    Example usage:
    ```python
    import pyarrow as pa
    import base64

    # Create Arrow table
    table = pa.table({'x': [1, 2, 3], 'y': [4, 5, 6]})

    # Serialize to IPC
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    data = base64.b64encode(sink.getvalue().to_pybytes()).decode()

    # Ingest via MCP
    result = await ingest_dataframe(IngestDataFrameRequest(
        name="my_analysis",
        data=data
    ))

    # Use virtual_dataset_id with generate_chart
    chart = await generate_chart(GenerateChartRequest(
        dataset_id=f"virtual:{result.dataset_id}",
        config={...}
    ))
    ```
    """
```

**Phase 3: Virtual Dataset Query Adapter (Week 3-4)**

Objective: Create an Explorable implementation for virtual datasets.

```python
# Proposed: superset/mcp_service/dataframe/virtual_explorable.py
from superset.explorables.base import Explorable
import duckdb

class VirtualDataFrameExplorable:
    """Explorable implementation for in-memory DataFrames."""

    def __init__(self, virtual_dataset: VirtualDataset):
        self._dataset = virtual_dataset
        self._duckdb = duckdb.connect()
        # Register Arrow table with DuckDB for SQL queries
        self._duckdb.register("data", virtual_dataset.table)

    def get_query_result(self, query_obj: QueryObject) -> QueryResult:
        """Execute query using DuckDB against Arrow table."""
        # Translate QueryObject to SQL
        sql = self._build_sql(query_obj)
        # Execute against DuckDB
        result = self._duckdb.execute(sql).arrow()
        return QueryResult(df=result.to_pandas(), ...)

    @property
    def columns(self) -> list[Any]:
        """Return column metadata from Arrow schema."""
        return [
            {
                "column_name": field.name,
                "type": str(field.type),
                "is_dttm": pa.types.is_temporal(field.type),
            }
            for field in self._dataset.schema
        ]
```

**Phase 4: Chart Generation Integration (Week 4-5)**

Objective: Extend `generate_chart` to support virtual datasets.

```python
# Modifications to superset/mcp_service/chart/tool/generate_chart.py

async def generate_chart(request: GenerateChartRequest, ctx: Context):
    """Extended to support virtual datasets."""

    dataset_id = request.dataset_id

    # Check if this is a virtual dataset reference
    if isinstance(dataset_id, str) and dataset_id.startswith("virtual:"):
        virtual_id = dataset_id[8:]  # Remove "virtual:" prefix

        # Get virtual dataset from registry
        from superset.mcp_service.dataframe.registry import get_registry
        registry = get_registry()
        virtual_dataset = registry.get(virtual_id)

        if not virtual_dataset:
            return error_response("Virtual dataset not found or expired")

        # Create chart with virtual explorable
        explorable = VirtualDataFrameExplorable(virtual_dataset)
        # ... continue with chart generation using explorable
    else:
        # Existing database dataset logic
        ...
```

**Phase 5: Fast Dashboard Pipeline (Week 5-6)**

Objective: Create an end-to-end pipeline for DataFrame-to-Dashboard.

```python
# Proposed: superset/mcp_service/dataframe/tool/create_dashboard_from_dataframe.py

class CreateDashboardFromDataFrameRequest(BaseModel):
    """Create a complete dashboard from DataFrame data."""
    name: str
    data: str  # Base64 Arrow IPC
    charts: List[ChartSpec]
    layout: Optional[LayoutSpec] = None
    auto_suggest: bool = Field(
        default=True,
        description="Auto-suggest charts based on data analysis")

@tool(tags=["dataframe", "dashboard", "mutate"])
async def create_dashboard_from_dataframe(
    request: CreateDashboardFromDataFrameRequest,
    ctx: Context
) -> CreateDashboardFromDataFrameResponse:
    """
    Create a complete dashboard directly from DataFrame data.

    This is a high-level tool that combines:
    1. DataFrame ingestion
    2. Chart generation
    3. Dashboard assembly

    Into a single operation for maximum efficiency.

    When auto_suggest=True, the tool analyzes the data and
    suggests appropriate visualizations based on:
    - Column types (temporal, categorical, numeric)
    - Data cardinality
    - Statistical properties
    """
```

### 5.3 DuckDB Integration for DataFrame Queries

Why DuckDB?

DuckDB provides crucial capabilities:

- Zero-copy Arrow Integration: Native support for querying Arrow tables.
- SQL Compatibility: Familiar SQL syntax for complex queries.
- In-Process: No network overhead for queries.
- Rich Analytics: Window functions, aggregations, joins.

Example DuckDB usage for virtual datasets:

```python
import duckdb
import pyarrow as pa

def query_arrow_table(table: pa.Table, sql: str) -> pa.Table:
    """Execute SQL against Arrow table using DuckDB."""
    conn = duckdb.connect()
    conn.register("df", table)

    # DuckDB returns Arrow directly
    result = conn.execute(sql).arrow()
    return result
```

### 5.4 Security Considerations

| Concern | Mitigation |
| --- | --- |
| Resource exhaustion | TTL-based cleanup, size limits |
| Data isolation | Session-scoped datasets by default |
| Permission bypass | Virtual datasets inherit session permissions |
| Memory limits | Configurable max dataset size per session |
| Data leakage | Auto-cleanup on session end |

### 5.5 Configuration Options

```python
# Proposed config additions for superset_config.py

# Virtual Dataset Configuration
MCP_VIRTUAL_DATASET_ENABLED = True
MCP_VIRTUAL_DATASET_MAX_SIZE_MB = 100  # Max size per dataset
MCP_VIRTUAL_DATASET_MAX_COUNT = 10  # Max datasets per session
MCP_VIRTUAL_DATASET_DEFAULT_TTL_MINUTES = 60
MCP_VIRTUAL_DATASET_STORAGE_BACKEND = "memory"  # or "redis" for multi-pod
```

## 6. Implementation Priority Matrix

| Phase | Effort | Impact | Priority |
| --- | --- | --- | --- |
| 1. Virtual Dataset Registry | Medium | High | P0 |
| 2. DataFrame Ingestion Tool | Medium | High | P0 |
| 3. DuckDB Query Adapter | High | High | P1 |
| 4. Chart Generation Integration | Medium | High | P1 |
| 5. Fast Dashboard Pipeline | Medium | Medium | P2 |
| 6. Auto-Chart Suggestion | High | Medium | P3 |

## 7. Success Metrics

### Performance Targets

- DataFrame ingestion: < 100ms for 1M rows.
- Chart generation from virtual dataset: < 500ms.
- Full dashboard creation: < 2s for 5 charts.

### Adoption Metrics

- Number of virtual datasets created per day.
- Conversion rate: virtual → persisted datasets.
- MCP tool usage frequency.

## 8. Risks and Mitigations

| Risk | Impact | Probability | Mitigation |
| --- | --- | --- | --- |
| Memory pressure from large datasets | High | Medium | Strict size limits, spill to disk |
| Session state complexity | Medium | High | Simple TTL-based cleanup |
| DuckDB compatibility issues | Medium | Low | Fallback to pandas processing |
| Security vulnerabilities | High | Low | Sandboxed execution, input validation |

## 9. Future Considerations

### DataFusion Integration

Apache DataFusion could replace DuckDB for potential benefits:

- Native Rust performance.
- Better Arrow integration.
- Async query execution.

### Streaming DataFrame Support

For very large datasets:

- Chunked ingestion via Arrow `RecordBatch` streams.
- Progressive visualization rendering.
- Out-of-core processing.

### Federated Queries

Combining virtual datasets with database datasets:

- Join in-memory data with database tables.
- Unified query optimization.

## 10. Conclusion

Apache Superset has a solid foundation for DataFrame/Arrow support at its core, but requires new components to fully realize DataFrame ingestion capabilities. The proposed MCP DataFrame interface provides a pragmatic path to enabling AI agents to create dashboards directly from DataFrame data without requiring database round-trips.

The phased implementation approach allows for:

- Quick wins with basic ingestion support.
- Progressive enhancement of query capabilities.
- Future-proof architecture for streaming and federation.

Recommended next steps:

- Implement Phase 1 (Virtual Dataset Registry) as a proof-of-concept.
- Validate DuckDB query performance with real-world data.
- Gather feedback from MCP service users on API design.
- Iterate on the implementation based on usage patterns.
