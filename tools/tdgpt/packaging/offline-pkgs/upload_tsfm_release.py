#!/usr/bin/env python3
"""Publish all files referenced by a TDgpt TSFM release manifest."""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_ENDPOINT = "https://cmd.tdengine.net/upload-any"
DEFAULT_CONTAINER = "tdengine-tdgpt-enterprise"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload TDgpt TSFM runtime/model packages listed in a release manifest."
    )
    parser.add_argument(
        "--release-manifest",
        type=Path,
        required=True,
        help="Local tdengine-tdgpt-tsfm-release-<version>.json file.",
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="Azure container prefix, such as tsfm/1.0.0. Defaults to tsfm/<release>.",
    )
    parser.add_argument("--container", default=DEFAULT_CONTAINER)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-delay", type=float, default=5.0)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--task-timeout", type=float, default=7200.0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def load_manifest(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Cannot read release manifest {path}: {exc}") from exc
    if value.get("schema_version") != 1 or not isinstance(value.get("release"), str):
        raise SystemExit("Unsupported TDgpt TSFM release manifest")
    return value


def resource_files(manifest: dict[str, Any]) -> list[str]:
    files: list[str] = []
    for item in [*manifest.get("runtimes", []), *manifest.get("models", [])]:
        if not isinstance(item, dict) or not isinstance(item.get("file"), str):
            raise SystemExit("Release manifest contains a resource without a file name")
        filename = item["file"]
        if not filename or Path(filename).name != filename:
            raise SystemExit(f"Unsafe resource file name in release manifest: {filename!r}")
        if filename not in files:
            files.append(filename)
    return files


def post_upload(
    endpoint: str, container: str, filename: str, retries: int, delay: float
) -> dict[str, Any]:
    payload = json.dumps({"filename": filename, "container": container}).encode("utf-8")
    request = urllib.request.Request(
        endpoint,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=1800) as response:
                body = response.read().decode("utf-8", errors="replace").strip()
                try:
                    result = json.loads(body)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"upload service returned invalid JSON: {body}") from exc
                if not isinstance(result, dict) or not result.get("task_id"):
                    raise RuntimeError(f"upload service did not return a task ID: {result}")
                return result
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            retryable = not isinstance(exc, urllib.error.HTTPError) or exc.code >= 500
            if not retryable or attempt == retries:
                raise RuntimeError(f"upload failed: {exc}") from exc
            print(f"Retry {attempt}/{retries - 1} for {filename}: {exc}")
            time.sleep(delay)
    raise AssertionError("unreachable")


def wait_for_task(endpoint: str, task_id: str, poll_interval: float, timeout: float) -> dict[str, Any]:
    task_endpoint = f"{endpoint.rsplit('/', 1)[0]}/tasks/{task_id}"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(task_endpoint, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"cannot poll upload task {task_id}: {exc}") from exc
        status = result.get("status")
        if status == "success":
            return result
        if status in {"failed", "error", "cancelled"}:
            raise RuntimeError(f"upload task {task_id} ended with {status}: {result.get('error')}")
        print(f"  task {task_id}: {status}")
        time.sleep(poll_interval)
    raise RuntimeError(f"upload task {task_id} timed out after {timeout:.0f}s")


def main() -> int:
    args = parse_args()
    if args.retries < 1:
        raise SystemExit("--retries must be at least 1")
    manifest = load_manifest(args.release_manifest)
    prefix = (args.prefix or f"tsfm/{manifest['release']}").strip("/")
    if not prefix:
        raise SystemExit("--prefix must not be empty")

    files = [f"{prefix}/{filename}" for filename in resource_files(manifest)]
    manifest_name = f"{prefix}/{args.release_manifest.name}"
    print(f"Container: {args.container}")
    print(f"Endpoint: {args.endpoint}")
    print("All upload tasks are submitted before polling their results")

    tasks: list[tuple[str, str]] = []
    for index, filename in enumerate([*files, manifest_name], start=1):
        print(f"[{index}/{len(files) + 1}] {filename}")
        if args.dry_run:
            continue
        response = post_upload(
            args.endpoint, args.container, filename, args.retries, args.retry_delay
        )
        task_id = str(response["task_id"])
        tasks.append((filename, task_id))
        print(f"  task {task_id}: submitted")

    for filename, task_id in tasks:
        print(f"Waiting for {filename}")
        task = wait_for_task(
            args.endpoint,
            task_id,
            args.poll_interval,
            args.task_timeout,
        )
        print(f"  task {task['task_id']}: {task['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
