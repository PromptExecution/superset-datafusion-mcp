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
Query Virtual Dataset MCP Tool

Allows executing SQL queries against virtual datasets using DuckDB.
"""

from __future__ import annotations

import logging
from typing import Any

from fastmcp import Context
from superset_core.mcp import tool

from superset.mcp_service.dataframe.registry import get_registry
from superset.mcp_service.dataframe.schemas import (
    QueryVirtualDatasetRequest,
    QueryVirtualDatasetResponse,
)
from superset.mcp_service.utils.schema_utils import parse_request
from superset.utils.core import get_user_id

logger = logging.getLogger(__name__)


def _convert_value(value: Any) -> str | int | float | bool | None:
    """Convert a value to a JSON-serializable type."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    # Handle numpy types and other types
    try:
        # Try to convert to float first (handles numpy numeric types)
        return float(value)
    except (TypeError, ValueError):
        pass
    # Fall back to string conversion
    return str(value)


@tool(tags=["dataframe"])
@parse_request(QueryVirtualDatasetRequest)
async def query_virtual_dataset(
    request: QueryVirtualDatasetRequest, ctx: Context
) -> QueryVirtualDatasetResponse:
    """
    Execute a SQL query against a virtual dataset.

    This tool uses DuckDB to execute SQL queries against in-memory
    virtual datasets. The virtual dataset is available as the 'data' table.

    Example queries:
    - SELECT * FROM data LIMIT 10
    - SELECT region, SUM(sales) as total FROM data GROUP BY region
    - SELECT date, sales FROM data WHERE sales > 100 ORDER BY date

    Args:
        dataset_id: The virtual dataset ID (without 'virtual:' prefix)
        sql: SQL query to execute
        limit: Maximum rows to return (default 1000)

    Returns:
        Query results as rows with column metadata.
    """
    await ctx.info(
        "Querying virtual dataset: dataset_id=%s, limit=%d"
        % (request.dataset_id, request.limit)
    )

    try:
        # Get the virtual dataset
        registry = get_registry()

        # Determine session and user context
        session_id = getattr(ctx, "session_id", None)
        try:
            user_id = get_user_id()
        except Exception:
            user_id = None

        # Avoid falling back to a shared default session identifier
        if not session_id:
            if user_id is not None:
                # Derive a per-user session identifier to prevent collisions
                session_id = f"user_session_{user_id}"
            else:
                logger.warning(
                    "Missing session_id and user_id; refusing to query virtual dataset "
                    "for dataset_id=%s",
                    request.dataset_id,
                )
                return QueryVirtualDatasetResponse(
                    success=False,
                    error=(
                        "Cannot query virtual dataset: missing session and user "
                        "context. Please retry after re-authenticating."
                    ),
                )
        dataset = registry.get(
            request.dataset_id, session_id=session_id, user_id=user_id
        )

        if dataset is None:
            return QueryVirtualDatasetResponse(
                success=False,
                error=(
                    f"Virtual dataset '{request.dataset_id}' not found, expired, "
                    "or access denied"
                ),
            )

        # Try to use DuckDB for SQL execution
        try:
            import duckdb
        except ImportError:
            logger.error("DuckDB not installed - required for virtual dataset queries")
            return QueryVirtualDatasetResponse(
                success=False,
                error=(
                    "DuckDB is required for virtual dataset queries. "
                    "Install with: pip install duckdb"
                ),
            )

        # Create DuckDB connection and register the table
        conn = duckdb.connect()
        conn.register("data", dataset.table)

        # Execute query with limit
        sql = request.sql.strip()

        # Add LIMIT if not present (safety measure)
        sql_upper = sql.upper()
        if "LIMIT" not in sql_upper:
            sql = f"{sql} LIMIT {request.limit}"

        # Remove a trailing semicolon so the query can be safely wrapped
        if sql.endswith(";"):
            sql = sql[:-1].rstrip()

        # Always enforce an outer LIMIT to cap the result size
        sql = f"SELECT * FROM ({sql}) AS subq"

        await ctx.debug("Executing SQL: %s" % sql[:200])

        try:
            result = conn.execute(sql)
            result_table = result.arrow()
        except duckdb.Error as e:
            logger.error("DuckDB query error: %s", e)
            return QueryVirtualDatasetResponse(
                success=False,
                error=f"SQL execution error: {str(e)}",
            )
        finally:
            conn.close()

        # Convert to response format
        df = result_table.to_pandas()
        rows = []
        for _, row in df.iterrows():
            rows.append({col: _convert_value(row[col]) for col in df.columns})

        columns = [{"name": col, "type": str(df[col].dtype)} for col in df.columns]

        await ctx.info(
            "Query completed: rows=%d, columns=%d" % (len(rows), len(columns))
        )

        return QueryVirtualDatasetResponse(
            success=True,
            rows=rows,
            columns=columns,
            row_count=len(rows),
        )

    except Exception as e:
        logger.exception("Error querying virtual dataset: %s", e)
        await ctx.error("Query failed: %s" % str(e))
        return QueryVirtualDatasetResponse(
            success=False,
            error=f"Unexpected error: {str(e)}",
        )
