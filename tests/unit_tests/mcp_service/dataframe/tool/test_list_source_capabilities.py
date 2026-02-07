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

from collections.abc import Generator
from unittest.mock import Mock, patch

import pytest
from fastmcp import Client

from superset.mcp_service.dataframe.schemas import ListSourceCapabilitiesRequest
from superset.mcp_service.app import mcp
from superset.utils import json


@pytest.fixture(autouse=True)
def mock_auth() -> Generator[None, None, None]:
    """Mock authentication for all tests."""
    with patch("superset.mcp_service.auth.get_user_from_request") as mock_get_user:
        mock_user = Mock()
        mock_user.id = 1
        mock_user.username = "admin"
        mock_get_user.return_value = mock_user
        yield


@pytest.fixture
def mcp_server():
    return mcp


@pytest.mark.asyncio
async def test_list_source_capabilities_default(mcp_server) -> None:
    """Default capability listing includes DataFusion and Prometheus sources."""
    async with Client(mcp_server) as client:
        result = await client.call_tool(
            "list_source_capabilities",
            {"request": ListSourceCapabilitiesRequest().model_dump()},
        )

    data = json.loads(result.content[0].text)
    assert data["success"] is True
    source_types = {cap["source_type"] for cap in data["capabilities"]}
    assert {
        "parquet",
        "arrow_ipc",
        "virtual_dataset",
        "prometheus_http",
    } <= source_types


@pytest.mark.asyncio
async def test_list_source_capabilities_filtered(mcp_server) -> None:
    """Filtering source types and Prometheus flag scopes the capability output."""
    async with Client(mcp_server) as client:
        result = await client.call_tool(
            "list_source_capabilities",
            {
                "request": ListSourceCapabilitiesRequest(
                    source_types=["parquet"],
                    include_prometheus=False,
                ).model_dump()
            },
        )

    data = json.loads(result.content[0].text)
    assert data["success"] is True
    assert data["total_count"] == 1
    assert data["capabilities"][0]["source_type"] == "parquet"
