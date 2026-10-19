# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import os
from typing import Any

from huggingface_hub import snapshot_download

DEFAULT_HF_MIRROR_ENDPOINT = "https://hf-mirror.com"
OFFICIAL_HF_ENDPOINT = "https://huggingface.co"
RETRIABLE_ENDPOINT_ERROR_NAMES = {
    "FileMetadataError",
    "LocalEntryNotFoundError",
}


def parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off", ""}:
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


def resolve_endpoint(enable_ep: object) -> str | None:
    if not parse_bool(enable_ep):
        return None
    return (
        os.environ.get("TAOS_HF_ENDPOINT")
        or os.environ.get("HF_ENDPOINT")
        or DEFAULT_HF_MIRROR_ENDPOINT
    )


def is_official_endpoint(endpoint: str | None) -> bool:
    effective_endpoint = (
        endpoint or os.environ.get("HF_ENDPOINT") or OFFICIAL_HF_ENDPOINT
    )
    return effective_endpoint.rstrip("/") == OFFICIAL_HF_ENDPOINT


def _matches_retriable_endpoint_error(exc: Exception) -> bool:
    return any(
        error_class.__name__ in RETRIABLE_ENDPOINT_ERROR_NAMES
        for error_class in type(exc).__mro__
    )


def _is_retriable_on_endpoint_change(exc: Exception) -> bool:
    """Return True if retrying with a different endpoint might succeed.

    Fallback is worthwhile for server-side errors (5xx) and network connectivity
    failures.  It is NOT worthwhile for:
    - HTTP 4xx errors: the remote resource has an issue independent of which
      endpoint is used (e.g. repo not found, gated repo, bad auth).
    - Local OS errors (PermissionError, disk full, etc.): switching endpoint
      cannot fix a local filesystem problem.
    """
    if _matches_retriable_endpoint_error(exc):
        return True
    response = getattr(exc, "response", None)
    if response is not None:
        status_code = getattr(response, "status_code", 0)
        if 400 <= status_code < 500:
            return False
        return True  # 5xx or unexpected HTTP error — server may be at fault
    # ConnectionError and TimeoutError are OSError subclasses but indicate
    # network issues that a different endpoint might resolve.
    if isinstance(exc, OSError) and not isinstance(
        exc, (ConnectionError, TimeoutError)
    ):
        return False  # local filesystem error
    return True


def snapshot_download_with_fallback(
    repo_id: str,
    local_dir: str,
    enable_ep: object = False,
    **kwargs: Any,
) -> str:
    kwargs.pop("endpoint", None)
    endpoint = resolve_endpoint(enable_ep)
    if endpoint:
        print(f"set the download ep:{endpoint}")
    try:
        return snapshot_download(
            repo_id=repo_id,
            local_dir=local_dir,
            endpoint=endpoint,
            **kwargs,
        )
    except Exception as exc:
        if is_official_endpoint(endpoint) or not _is_retriable_on_endpoint_change(exc):
            raise
        failed_endpoint = endpoint or os.environ.get("HF_ENDPOINT")
        print(
            f"download from endpoint {failed_endpoint} failed: {exc}; "
            f"fallback to {OFFICIAL_HF_ENDPOINT}"
        )
        return snapshot_download(
            repo_id=repo_id,
            local_dir=local_dir,
            endpoint=OFFICIAL_HF_ENDPOINT,
            **kwargs,
        )
