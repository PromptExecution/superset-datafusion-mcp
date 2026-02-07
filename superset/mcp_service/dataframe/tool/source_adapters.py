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
Source adapter abstractions for dataframe tools.

This module formalizes source registration behavior and capability metadata
for DataFusion and related MCP dataframe tools.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Protocol

import pyarrow as pa

from superset.mcp_service.dataframe.identifiers import normalize_virtual_dataset_id
from superset.mcp_service.dataframe.registry import VirtualDatasetRegistry
from superset.mcp_service.dataframe.schemas import DataFusionSourceConfig


def _register_arrow_table(session_ctx: Any, table_name: str, table: pa.Table) -> None:
    """Register an Arrow table in DataFusion across supported APIs."""
    batches = table.to_batches()

    if hasattr(session_ctx, "register_record_batches"):
        try:
            session_ctx.register_record_batches(table_name, [batches])
            return
        except TypeError:
            session_ctx.register_record_batches(table_name, batches)
            return

    if hasattr(session_ctx, "from_arrow_table"):
        df = session_ctx.from_arrow_table(table)
        if hasattr(df, "create_temp_view"):
            df.create_temp_view(table_name)
            return

    raise RuntimeError(
        "DataFusion runtime does not support Arrow table registration "
        "with available APIs"
    )


@dataclass(frozen=True)
class DataFrameSourceCapability:
    """Capability metadata for a dataframe source adapter."""

    source_type: str
    adapter_name: str
    supports_streaming: bool
    supports_projection_pushdown: bool
    supports_predicate_pushdown: bool
    supports_sql_pushdown: bool
    supports_virtual_dataset_ingestion: bool


class DataFrameSourceAdapter(Protocol):
    """Contract for dataframe source registration adapters."""

    source_type: str
    capability: DataFrameSourceCapability

    def register_source(
        self,
        session_ctx: Any,
        source: DataFusionSourceConfig,
        registry: VirtualDatasetRegistry,
        session_id: str | None,
        user_id: int | None,
    ) -> None:
        """Register a source in an active DataFusion SessionContext."""


class ParquetSourceAdapter:
    """Adapter for parquet data sources."""

    source_type = "parquet"
    capability = DataFrameSourceCapability(
        source_type=source_type,
        adapter_name="ParquetSourceAdapter",
        supports_streaming=True,
        supports_projection_pushdown=True,
        supports_predicate_pushdown=True,
        supports_sql_pushdown=True,
        supports_virtual_dataset_ingestion=True,
    )

    def register_source(
        self,
        session_ctx: Any,
        source: DataFusionSourceConfig,
        registry: VirtualDatasetRegistry,  # noqa: ARG002
        session_id: str | None,  # noqa: ARG002
        user_id: int | None,  # noqa: ARG002
    ) -> None:
        if source.path is None:
            raise ValueError("Missing parquet path in source configuration")
        session_ctx.register_parquet(source.name, source.path)


class ArrowIpcSourceAdapter:
    """Adapter for Arrow IPC payloads."""

    source_type = "arrow_ipc"
    capability = DataFrameSourceCapability(
        source_type=source_type,
        adapter_name="ArrowIpcSourceAdapter",
        supports_streaming=False,
        supports_projection_pushdown=False,
        supports_predicate_pushdown=False,
        supports_sql_pushdown=False,
        supports_virtual_dataset_ingestion=True,
    )

    def register_source(
        self,
        session_ctx: Any,
        source: DataFusionSourceConfig,
        registry: VirtualDatasetRegistry,  # noqa: ARG002
        session_id: str | None,  # noqa: ARG002
        user_id: int | None,  # noqa: ARG002
    ) -> None:
        if source.data is None:
            raise ValueError("Missing Arrow IPC payload in source configuration")
        raw_data = base64.b64decode(source.data)
        reader = pa.ipc.open_stream(pa.BufferReader(raw_data))
        table = reader.read_all()
        _register_arrow_table(session_ctx, source.name, table)


class VirtualDatasetSourceAdapter:
    """Adapter for registered MCP virtual datasets."""

    source_type = "virtual_dataset"
    capability = DataFrameSourceCapability(
        source_type=source_type,
        adapter_name="VirtualDatasetSourceAdapter",
        supports_streaming=False,
        supports_projection_pushdown=False,
        supports_predicate_pushdown=False,
        supports_sql_pushdown=False,
        supports_virtual_dataset_ingestion=True,
    )

    def register_source(
        self,
        session_ctx: Any,
        source: DataFusionSourceConfig,
        registry: VirtualDatasetRegistry,
        session_id: str | None,
        user_id: int | None,
    ) -> None:
        if source.dataset_id is None:
            raise ValueError("Missing dataset_id in source configuration")

        raw_dataset_id = normalize_virtual_dataset_id(source.dataset_id)
        dataset = registry.get(
            raw_dataset_id,
            session_id=session_id,
            user_id=user_id,
        )
        if dataset is None:
            raise ValueError(
                f"Virtual dataset '{source.dataset_id}' not found, "
                "expired, or access denied"
            )

        _register_arrow_table(session_ctx, source.name, dataset.table)


DATAFUSION_SOURCE_ADAPTERS: dict[str, DataFrameSourceAdapter] = {
    "parquet": ParquetSourceAdapter(),
    "arrow_ipc": ArrowIpcSourceAdapter(),
    "virtual_dataset": VirtualDatasetSourceAdapter(),
}

PROMETHEUS_SOURCE_CAPABILITY = DataFrameSourceCapability(
    source_type="prometheus_http",
    adapter_name="PrometheusHttpAdapter",
    supports_streaming=False,
    supports_projection_pushdown=False,
    supports_predicate_pushdown=True,
    supports_sql_pushdown=False,
    supports_virtual_dataset_ingestion=True,
)


def get_datafusion_source_adapter(source_type: str) -> DataFrameSourceAdapter | None:
    """Get the registered DataFusion source adapter for source_type."""
    return DATAFUSION_SOURCE_ADAPTERS.get(source_type)


def list_datafusion_source_capabilities(
    source_types: list[str] | None = None,
) -> list[DataFrameSourceCapability]:
    """List capability metadata for known DataFusion source adapters."""
    if source_types is None:
        source_types = list(DATAFUSION_SOURCE_ADAPTERS.keys())
    return [
        DATAFUSION_SOURCE_ADAPTERS[source_type].capability
        for source_type in source_types
        if source_type in DATAFUSION_SOURCE_ADAPTERS
    ]
