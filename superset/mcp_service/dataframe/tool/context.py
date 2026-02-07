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

from fastmcp import Context

from superset.utils.core import get_user_id


def resolve_session_and_user(ctx: Context) -> tuple[str | None, int | None]:
    """
    Resolve session and user identity for DataFrame virtual dataset access.

    Falls back to a deterministic per-user session identifier when FastMCP
    session context is unavailable.
    """
    session_id = getattr(ctx, "session_id", None)
    try:
        user_id = get_user_id()
    except Exception:
        user_id = None

    if not session_id and user_id is not None:
        session_id = f"user_{user_id}"

    return session_id, user_id
