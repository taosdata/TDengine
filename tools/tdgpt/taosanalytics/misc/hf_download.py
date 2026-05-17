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
    if not endpoint:
        return True
    return endpoint.rstrip("/") == OFFICIAL_HF_ENDPOINT


def snapshot_download_with_fallback(
    *,
    repo_id: str,
    local_dir: str,
    enable_ep: object = False,
    **kwargs: Any,
) -> str:
    endpoint = resolve_endpoint(enable_ep)
    print(f"set the download ep:{endpoint}")
    try:
        return snapshot_download(
            repo_id=repo_id,
            local_dir=local_dir,
            endpoint=endpoint,
            **kwargs,
        )
    except Exception as exc:
        if is_official_endpoint(endpoint):
            raise
        print(
            f"download from endpoint {endpoint} failed: {exc}; "
            f"fallback to {OFFICIAL_HF_ENDPOINT}"
        )
        return snapshot_download(
            repo_id=repo_id,
            local_dir=local_dir,
            endpoint=OFFICIAL_HF_ENDPOINT,
            **kwargs,
        )
