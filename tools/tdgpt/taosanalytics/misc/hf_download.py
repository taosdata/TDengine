import os
from typing import Any, Optional

from huggingface_hub import snapshot_download


DEFAULT_HF_MIRROR_ENDPOINT = "https://hf-mirror.com"
OFFICIAL_HF_ENDPOINT = "https://huggingface.co"


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


def resolve_endpoint(enable_ep: object) -> Optional[str]:
    if not parse_bool(enable_ep):
        return None
    return (
        os.environ.get("TAOS_HF_ENDPOINT")
        or os.environ.get("HF_ENDPOINT")
        or DEFAULT_HF_MIRROR_ENDPOINT
    )


def is_official_endpoint(endpoint: Optional[str]) -> bool:
    effective_endpoint = endpoint or os.environ.get("HF_ENDPOINT") or OFFICIAL_HF_ENDPOINT
    return effective_endpoint.rstrip("/") == OFFICIAL_HF_ENDPOINT


def _is_retriable_on_endpoint_change(exc: Exception) -> bool:
    """Return True if retrying with a different endpoint might succeed.

    Fallback is worthwhile for server-side errors (5xx) and network connectivity
    failures.  It is NOT worthwhile for:
    - HTTP 4xx errors: the remote resource has an issue independent of which
      endpoint is used (e.g. repo not found, gated repo, bad auth).
    - Local OS errors (PermissionError, disk full, etc.): switching endpoint
      cannot fix a local filesystem problem.
    """
    response = getattr(exc, "response", None)
    if response is not None:
        status_code = getattr(response, "status_code", 0)
        if 400 <= status_code < 500:
            return False
        return True  # 5xx or unexpected HTTP error — server may be at fault
    # ConnectionError and TimeoutError are OSError subclasses but indicate
    # network issues that a different endpoint might resolve.
    if isinstance(exc, OSError) and not isinstance(exc, (ConnectionError, TimeoutError)):
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
