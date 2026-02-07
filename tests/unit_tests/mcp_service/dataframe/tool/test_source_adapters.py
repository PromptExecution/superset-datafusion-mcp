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

from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa

from superset.mcp_service.dataframe.schemas import DataFusionSourceConfig
from superset.mcp_service.dataframe.tool.source_adapters import (
    get_datafusion_source_adapter,
    list_datafusion_source_capabilities,
)


def test_get_datafusion_source_adapter() -> None:
    """Known source adapters are discoverable by source_type."""
    assert get_datafusion_source_adapter("parquet") is not None
    assert get_datafusion_source_adapter("arrow_ipc") is not None
    assert get_datafusion_source_adapter("virtual_dataset") is not None
    assert get_datafusion_source_adapter("unknown") is None


def test_list_datafusion_source_capabilities() -> None:
    """Capability metadata is returned for supported source types."""
    capabilities = list_datafusion_source_capabilities()
    assert {cap.source_type for cap in capabilities} == {
        "parquet",
        "arrow_ipc",
        "virtual_dataset",
    }


def test_virtual_dataset_adapter_normalizes_prefixed_id() -> None:
    """Virtual adapter accepts prefixed IDs and resolves raw registry IDs."""
    adapter = get_datafusion_source_adapter("virtual_dataset")
    assert adapter is not None

    source = DataFusionSourceConfig(
        name="source_table",
        source_type="virtual_dataset",
        dataset_id="virtual:abc123",
    )
    registry = MagicMock()
    registry.get.return_value = SimpleNamespace(
        table=pa.table({"value": [1, 2, 3]}),
    )

    class SessionCtx:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def register_record_batches(self, table_name: str, batches: object) -> None:
            self.calls.append(table_name)

    session_ctx = SessionCtx()
    adapter.register_source(
        session_ctx=session_ctx,
        source=source,
        registry=registry,
        session_id="session_1",
        user_id=42,
    )

    registry.get.assert_called_once_with("abc123", session_id="session_1", user_id=42)
    assert session_ctx.calls == ["source_table"]
