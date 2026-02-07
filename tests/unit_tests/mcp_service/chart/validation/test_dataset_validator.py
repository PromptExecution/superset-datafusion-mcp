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
from unittest.mock import MagicMock, patch

import pyarrow as pa

from superset.mcp_service.chart.schemas import ColumnRef, TableChartConfig
from superset.mcp_service.chart.validation.dataset_validator import DatasetValidator


def test_get_dataset_context_from_virtual_dataset() -> None:
    """Virtual dataset identifiers resolve to virtual dataset context."""
    mock_registry = MagicMock()
    mock_registry.get.return_value = SimpleNamespace(
        id="abc123",
        name="virtual_sales",
        schema=pa.schema(
            [
                pa.field("event_time", pa.timestamp("ms")),
                pa.field("value", pa.float64()),
            ]
        ),
    )

    with patch(
        "superset.mcp_service.chart.validation.dataset_validator.get_registry",
        return_value=mock_registry,
    ):
        context = DatasetValidator._get_dataset_context(
            dataset_id="virtual:abc123",
            session_id="session_1",
            user_id=42,
        )

    assert context is not None
    assert context.id == "abc123"
    assert context.table_name == "virtual_sales"
    assert len(context.available_columns) == 2


def test_validate_against_virtual_dataset_columns() -> None:
    """Column validation uses virtual dataset schema for chart config checks."""
    mock_registry = MagicMock()
    mock_registry.get.return_value = SimpleNamespace(
        id="abc123",
        name="virtual_sales",
        schema=pa.schema([pa.field("region", pa.string())]),
    )

    config = TableChartConfig(
        chart_type="table",
        columns=[ColumnRef(name="missing_column")],
    )

    with patch(
        "superset.mcp_service.chart.validation.dataset_validator.get_registry",
        return_value=mock_registry,
    ):
        is_valid, error = DatasetValidator.validate_against_dataset(
            config,
            dataset_id="virtual:abc123",
            session_id="session_1",
            user_id=42,
        )

    assert not is_valid
    assert error is not None
    assert error.error_type == "column_not_found"
