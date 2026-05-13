#!/usr/bin/env python3
"""
查询 GitLab Runner 信息（name、tags、status）。

用法:
    python3 list-runners.py --token <PRIVATE-TOKEN> [--url <gitlab-base-url>]

示例:
    python3 list-runners.py --token glpat-xxxx
    python3 list-runners.py --token glpat-xxxx --url https://gitlab.example.com
"""

import argparse
import json
import sys
import urllib.request
from datetime import datetime


def fetch(url: str, token: str) -> dict | list:
    req = urllib.request.Request(url, headers={"PRIVATE-TOKEN": token})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def get_all_runners(base_url: str, token: str) -> list[dict]:
    page, runners = 1, []
    while True:
        url = f"{base_url}/api/v4/runners?per_page=100&page={page}"
        page_data = fetch(url, token)
        if not page_data:
            break
        runners.extend(page_data)
        if len(page_data) < 100:
            break
        page += 1
    return runners


def get_runner_detail(base_url: str, token: str, runner_id: int) -> dict:
    url = f"{base_url}/api/v4/runners/{runner_id}"
    return fetch(url, token)


def print_table(runners_detail: list[dict]) -> None:
    col_id   = 4
    col_name = 28
    col_st   = 10
    col_un   = 11
    header = (
        f"{'ID':>{col_id}}  "
        f"{'Name':<{col_name}}  "
        f"{'Status':<{col_st}}  "
        f"{'RunUntagged':<{col_un}}  "
        f"Tags"
    )
    print(header)
    print("-" * 100)
    for r in runners_detail:
        tags     = ", ".join(r.get("tag_list", []))
        run_un   = str(r.get("run_untagged", "?"))
        status   = r.get("status", "?")
        name     = r.get("description", "?")
        rid      = r.get("id", "?")
        print(
            f"{rid:>{col_id}}  "
            f"{name:<{col_name}}  "
            f"{status:<{col_st}}  "
            f"{run_un:<{col_un}}  "
            f"{tags}"
        )


def print_markdown(runners_detail: list[dict], base_url: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"# GitLab Runner 信息\n")
    print(f"> 查询时间：{now}  ")
    print(f"> GitLab 实例：`{base_url}`\n")
    print(f"| {'ID':>4} | {'Name':<28} | {'Status':<10} | {'RunUntagged':<11} | Tags |")
    print(f"|{'-'*6}|{'-'*30}|{'-'*12}|{'-'*13}|------|")
    for r in runners_detail:
        tags   = " ".join(f"`{t}`" for t in r.get("tag_list", []))
        run_un = str(r.get("run_untagged", "?"))
        status = r.get("status", "?")
        name   = r.get("description", "?")
        rid    = r.get("id", "?")
        print(f"| {rid:>4} | {name:<28} | {status:<10} | {run_un:<11} | {tags} |")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="列出 GitLab 所有 Runner 的名称、标签和状态"
    )
    parser.add_argument(
        "--token", "-t",
        required=True,
        help="GitLab Private Access Token (PRIVATE-TOKEN)"
    )
    parser.add_argument(
        "--url", "-u",
        default="https://git.tdengine.net",
        help="GitLab 实例地址，不含末尾斜杠（默认: https://git.tdengine.net）"
    )
    parser.add_argument(
        "--format", "-f",
        choices=["table", "markdown", "json"],
        default="table",
        help="输出格式：table（默认）| markdown | json"
    )
    args = parser.parse_args()

    base_url = args.url.rstrip("/")

    print(f"[*] 正在查询 {base_url} 的 Runner 列表 ...", file=sys.stderr)
    try:
        runners = get_all_runners(base_url, args.token)
    except Exception as e:
        print(f"[错误] 获取 Runner 列表失败: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"[*] 共找到 {len(runners)} 个 Runner，正在获取详情 ...", file=sys.stderr)
    details = []
    for r in runners:
        try:
            detail = get_runner_detail(base_url, args.token, r["id"])
            details.append(detail)
        except Exception as e:
            print(f"[警告] Runner {r['id']} 详情获取失败: {e}", file=sys.stderr)
            details.append(r)  # 降级使用列表接口数据

    if args.format == "table":
        print()
        print_table(details)
    elif args.format == "markdown":
        print_markdown(details, base_url)
    elif args.format == "json":
        print(json.dumps(details, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
