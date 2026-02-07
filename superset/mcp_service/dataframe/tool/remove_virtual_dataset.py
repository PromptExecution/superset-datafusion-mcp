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
Remove Virtual Dataset MCP Tool

Allows removing a virtual dataset from the registry before its TTL expires.
"""

from __future__ import annotations

import logging

from fastmcp import Context
from superset_core.mcp import tool

from superset.mcp_service.dataframe.registry import get_registry
from superset.mcp_service.dataframe.schemas import (
    RemoveVirtualDatasetRequest,
    RemoveVirtualDatasetResponse,
)
from superset.mcp_service.utils.schema_utils import parse_request
from superset.utils.core import get_user_id

logger = logging.getLogger(__name__)


@tool(tags=["dataframe", "mutate"])
@parse_request(RemoveVirtualDatasetRequest)
async def remove_virtual_dataset(
    request: RemoveVirtualDatasetRequest, ctx: Context
) -> RemoveVirtualDatasetResponse:
    """
    Remove a virtual dataset from the registry.

    This allows freeing up memory before the dataset's TTL expires.
    Useful when you've finished working with a dataset and want to
    immediately reclaim resources.

    Args:
        dataset_id: The virtual dataset ID to remove

    Returns:
        Success status and message.
    """
    await ctx.info("Removing virtual dataset: dataset_id=%s" % request.dataset_id)

    try:
        registry = get_registry()
        session_id = getattr(ctx, "session_id", None) or "default_session"
        try:
            user_id = get_user_id()
        except Exception:
            user_id = None

        # Check if dataset exists
        dataset = registry.get(
            request.dataset_id, session_id=session_id, user_id=user_id
        )
        if dataset is None:
            return RemoveVirtualDatasetResponse(
                success=False,
                message=(
                    f"Virtual dataset '{request.dataset_id}' "
                    "not found, already expired, or access denied"
                ),
            )

        # Remove the dataset
        removed = registry.remove(
            request.dataset_id, session_id=session_id, user_id=user_id
        )

        if removed:
            await ctx.info("Virtual dataset removed successfully")
            return RemoveVirtualDatasetResponse(
                success=True,
                message=(
                    f"Virtual dataset '{request.dataset_id}' removed successfully"
                ),
            )
        else:
            return RemoveVirtualDatasetResponse(
                success=False,
                message=f"Failed to remove virtual dataset '{request.dataset_id}'",
            )

    except Exception as e:
        logger.exception("Error removing virtual dataset: %s", e)
        await ctx.error("Failed to remove virtual dataset: %s" % str(e))
        return RemoveVirtualDatasetResponse(
            success=False,
            message=f"Error: {str(e)}",
        )
