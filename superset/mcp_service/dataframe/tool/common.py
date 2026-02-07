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

from __future__ import annotations

import math
import re
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

import pyarrow as pa

from superset.mcp_service.dataframe.registry import VirtualDataset
from superset.mcp_service.dataframe.schemas import VirtualDatasetInfo

UNSAFE_SQL_KEYWORDS = re.compile(
    r"\b("
    r"ALTER|ATTACH|CALL|COPY|CREATE|DELETE|DETACH|DROP|EXPORT|IMPORT|INSTALL|"
    r"INSERT|LOAD|PRAGMA|SET|UPDATE|VACUUM"
    r")\b",
    re.IGNORECASE,
)


def normalize_sql(sql: str) -> str:
    """Normalize SQL for safe wrapping and execution."""
    normalized_sql = sql.strip()
    if normalized_sql.endswith(";"):
        normalized_sql = normalized_sql[:-1].rstrip()
    return normalized_sql


def validate_read_only_sql(sql: str) -> str | None:
    """Validate SQL text to allow only single-statement read-only queries."""
    normalized_sql = normalize_sql(sql)
    if not normalized_sql:
        return "SQL query cannot be empty"
    if ";" in normalized_sql:
        return "Only single-statement SQL queries are allowed"

    normalized_upper = normalized_sql.upper()
    if not (
        normalized_upper.startswith("SELECT")
        or normalized_upper.startswith("WITH")
    ):
        return "Only SELECT and WITH queries are allowed"
    if UNSAFE_SQL_KEYWORDS.search(normalized_sql):
        return "Query contains non-read-only SQL keywords"

    return None


def convert_value(value: Any) -> str | int | float | bool | None:
    """Convert a scalar to a JSON-safe primitive representation."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        numeric = float(value)
        if math.isfinite(numeric):
            return numeric
        return str(value)

    try:
        numeric = float(value)
        if math.isfinite(numeric):
            return numeric
    except (TypeError, ValueError):
        pass

    return str(value)


def table_to_rows(
    table: pa.Table,
) -> list[dict[str, str | int | float | bool | None]]:
    """Convert an Arrow table into JSON-safe rows."""
    raw_rows = table.to_pylist()
    return [
        {
            column_name: convert_value(column_value)
            for column_name, column_value in row.items()
        }
        for row in raw_rows
    ]


def table_to_columns(table: pa.Table) -> list[dict[str, str]]:
    """Build typed column metadata from an Arrow table schema."""
    return [{"name": field.name, "type": str(field.type)} for field in table.schema]


def build_virtual_dataset_info(
    dataset: VirtualDataset,
    description: str | None = None,
) -> VirtualDatasetInfo:
    """Build a response model for a registered virtual dataset."""
    return VirtualDatasetInfo(
        id=dataset.id,
        name=dataset.name,
        row_count=dataset.row_count,
        column_count=len(dataset.column_names),
        size_bytes=dataset.size_bytes,
        size_mb=round(dataset.size_bytes / 1024 / 1024, 2),
        created_at=dataset.created_at,
        expires_at=dataset.expires_at,
        columns=dataset.get_column_info(),
        description=description,
    )
