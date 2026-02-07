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
List DataFrame Source Capabilities MCP Tool

Returns discoverability metadata for supported dataframe source adapters.
"""

from __future__ import annotations

import logging

from fastmcp import Context
from superset_core.mcp import tool

from superset.mcp_service.dataframe.schemas import (
    DataFrameSourceCapability,
    ListSourceCapabilitiesRequest,
    ListSourceCapabilitiesResponse,
)
from superset.mcp_service.dataframe.tool.source_adapters import (
    PROMETHEUS_SOURCE_CAPABILITY,
    list_datafusion_source_capabilities,
)
from superset.mcp_service.utils.schema_utils import parse_request

logger = logging.getLogger(__name__)

PROMETHEUS_SOURCE_TYPE = "prometheus_http"


@tool(tags=["dataframe", "metadata"])
@parse_request(ListSourceCapabilitiesRequest)
async def list_source_capabilities(
    request: ListSourceCapabilitiesRequest, ctx: Context
) -> ListSourceCapabilitiesResponse:
    """
    List source capability metadata for dataframe tools.

    This helps agents decide which connector path to use based on support for:
    - streaming
    - projection/predicate pushdown
    - SQL pushdown
    - virtual dataset ingestion
    """
    await ctx.info("Listing dataframe source capabilities")

    try:
        requested_types = (
            {value.strip() for value in request.source_types if value.strip()}
            if request.source_types
            else None
        )

        datafusion_filter = None
        if requested_types is not None:
            datafusion_filter = [
                source_type
                for source_type in requested_types
                if source_type != PROMETHEUS_SOURCE_TYPE
            ]

        capabilities = [
            DataFrameSourceCapability.model_validate(capability.__dict__)
            for capability in list_datafusion_source_capabilities(datafusion_filter)
        ]

        include_prometheus = request.include_prometheus and (
            requested_types is None or PROMETHEUS_SOURCE_TYPE in requested_types
        )
        if include_prometheus:
            capabilities.append(
                DataFrameSourceCapability.model_validate(
                    PROMETHEUS_SOURCE_CAPABILITY.__dict__
                )
            )

        await ctx.info("Listed %d source capabilities" % len(capabilities))
        return ListSourceCapabilitiesResponse(
            success=True,
            capabilities=capabilities,
            total_count=len(capabilities),
        )
    except Exception as ex:
        logger.exception("Failed to list source capabilities: %s", ex)
        await ctx.error("Failed to list source capabilities: %s" % str(ex))
        return ListSourceCapabilitiesResponse(
            success=False,
            capabilities=[],
            total_count=0,
            error=str(ex),
        )
