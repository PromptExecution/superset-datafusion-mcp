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
Bridge helpers for using MCP virtual datasets in chart workflows.
"""

from __future__ import annotations

from typing import Any

from superset.mcp_service.dataframe.identifiers import (
    extract_virtual_dataset_id,
)
from superset.mcp_service.dataframe.registry import (
    VirtualDataset,
    get_registry,
)
from superset.mcp_service.dataframe.tool.common import table_to_columns, table_to_rows

SUPPORTED_FILTER_OPERATORS = {"=", "==", ">", "<", ">=", "<=", "!="}


def resolve_virtual_dataset(
    dataset_id: int | str,
    session_id: str | None,
    user_id: int | None,
) -> VirtualDataset | None:
    """Resolve a prefixed virtual dataset identifier to a registry entry."""
    raw_dataset_id = extract_virtual_dataset_id(dataset_id)
    if raw_dataset_id is None:
        return None
    registry = get_registry()
    return registry.get(raw_dataset_id, session_id=session_id, user_id=user_id)


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _to_sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _build_where_clause(form_data: dict[str, Any]) -> str:
    filters = form_data.get("adhoc_filters") or []
    clauses: list[str] = []
    for filter_config in filters:
        if not isinstance(filter_config, dict):
            continue
        if filter_config.get("expressionType") != "SIMPLE":
            continue
        column = filter_config.get("subject")
        operator = filter_config.get("operator")
        comparator = filter_config.get("comparator")
        if not isinstance(column, str):
            continue
        if not isinstance(operator, str):
            continue
        if operator not in SUPPORTED_FILTER_OPERATORS:
            continue

        normalized_operator = "=" if operator == "==" else operator
        if comparator is None:
            if normalized_operator == "=":
                clauses.append(f"{_quote_identifier(column)} IS NULL")
            elif normalized_operator == "!=":
                clauses.append(f"{_quote_identifier(column)} IS NOT NULL")
            continue

        clauses.append(
            f"{_quote_identifier(column)} {normalized_operator} "
            f"{_to_sql_literal(comparator)}"
        )

    if not clauses:
        return ""
    return " WHERE " + " AND ".join(clauses)


def _unique_dimension_columns(form_data: dict[str, Any]) -> list[str]:
    columns: list[str] = []

    query_mode = form_data.get("query_mode")
    if query_mode == "raw":
        raw_columns = form_data.get("all_columns") or form_data.get("columns") or []
        for value in raw_columns:
            if isinstance(value, str) and value not in columns:
                columns.append(value)
        return columns

    x_axis = form_data.get("x_axis")
    if isinstance(x_axis, str) and x_axis not in columns:
        columns.append(x_axis)
    elif isinstance(x_axis, dict):
        x_name = x_axis.get("column_name")
        if isinstance(x_name, str) and x_name not in columns:
            columns.append(x_name)

    for group_column in form_data.get("groupby", []) or []:
        if isinstance(group_column, str) and group_column not in columns:
            columns.append(group_column)

    return columns


def _metric_sql_expressions(form_data: dict[str, Any]) -> list[str]:
    metrics = form_data.get("metrics") or []
    expressions: list[str] = []

    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        aggregate = str(metric.get("aggregate") or "SUM").upper()
        if aggregate not in {
            "SUM",
            "COUNT",
            "AVG",
            "MIN",
            "MAX",
            "COUNT_DISTINCT",
            "STDDEV",
            "VAR",
            "MEDIAN",
            "PERCENTILE",
        }:
            continue

        column_name: str | None = None
        metric_column = metric.get("column")
        if isinstance(metric_column, dict):
            raw_column_name = metric_column.get("column_name")
            if isinstance(raw_column_name, str):
                column_name = raw_column_name

        if aggregate == "COUNT" and column_name is None:
            base_expression = "COUNT(*)"
            default_label = "COUNT(*)"
        else:
            if column_name is None:
                continue
            quoted_column = _quote_identifier(column_name)
            if aggregate == "COUNT_DISTINCT":
                base_expression = f"COUNT(DISTINCT {quoted_column})"
            elif aggregate == "PERCENTILE":
                base_expression = f"QUANTILE_CONT({quoted_column}, 0.5)"
            else:
                base_expression = f"{aggregate}({quoted_column})"
            default_label = f"{aggregate}({column_name})"

        label = metric.get("label")
        alias = label if isinstance(label, str) and label else default_label
        expressions.append(f"{base_expression} AS {_quote_identifier(alias)}")

    return expressions


def build_virtual_dataset_query(form_data: dict[str, Any], limit: int = 1000) -> str:
    """Build a DuckDB SQL query from Superset form_data for virtual datasets."""
    safe_limit = max(1, min(limit, 10000))
    where_clause = _build_where_clause(form_data)
    query_mode = form_data.get("query_mode")

    if query_mode == "raw":
        columns = _unique_dimension_columns(form_data)
        select_clause = (
            ", ".join(_quote_identifier(column) for column in columns) if columns else "*"
        )
        return f"SELECT {select_clause} FROM data{where_clause} LIMIT {safe_limit}"

    dimensions = _unique_dimension_columns(form_data)
    metrics = _metric_sql_expressions(form_data)
    select_parts = [
        *(_quote_identifier(column) for column in dimensions),
        *metrics,
    ]
    if not select_parts:
        select_parts = ["*"]

    sql = f"SELECT {', '.join(select_parts)} FROM data{where_clause}"
    if dimensions and metrics:
        sql += " GROUP BY " + ", ".join(_quote_identifier(column) for column in dimensions)
    sql += f" LIMIT {safe_limit}"
    return sql


def query_virtual_dataset_with_form_data(
    dataset: VirtualDataset,
    form_data: dict[str, Any],
    limit: int = 1000,
) -> tuple[list[dict[str, str | int | float | bool | None]], list[dict[str, str]]]:
    """
    Execute a chart-style query for a virtual dataset and return rows/columns.
    """
    try:
        import duckdb
    except ImportError as ex:
        raise RuntimeError(
            "DuckDB is required to render chart previews for virtual datasets. "
            "Install with: pip install duckdb"
        ) from ex

    sql = build_virtual_dataset_query(form_data, limit=limit)
    connection = duckdb.connect()
    connection.register("data", dataset.table)
    try:
        result = connection.execute(sql)
        result_table = result.arrow()
    finally:
        connection.close()

    rows = table_to_rows(result_table)
    columns = table_to_columns(result_table)
    return rows, columns
