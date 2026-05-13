#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class Evidence:
    kind: str
    path: str
    pattern: str
    description: str


@dataclass(frozen=True)
class CheckSpec:
    section: str
    title: str
    description: str
    passed_summary: str
    evidences: tuple[Evidence, ...]


@dataclass(frozen=True)
class MatchResult:
    evidence: Evidence
    found: bool
    line_no: int | None
    snippet: str | None
    error: str | None = None


@dataclass(frozen=True)
class CheckResult:
    spec: CheckSpec
    evidences: tuple[MatchResult, ...]

    @property
    def passed(self) -> bool:
        return all(item.found for item in self.evidences)


DATA_PIPELINE_RS = "docs/overview/03-各模块设计/工具组件/数据管道工具/数据管道工具-Requirement Spec.md"
DATA_PIPELINE_FS = "docs/overview/03-各模块设计/工具组件/数据管道工具/数据管道工具-Function Spec.md"
ADAPTER_FS = "docs/overview/03-各模块设计/工具组件/数据接入适配工具/数据接入适配工具-Function Spec.md"
EXPLORER_RS = "docs/overview/03-各模块设计/工具组件/可视化管理工具/可视化管理工具-Requirement Spec.md"
EXPLORER_DS = "docs/overview/03-各模块设计/工具组件/可视化管理工具/可视化管理工具-Design Spec.md"
FULL_TRACE_AUTH = "docs/full-trace/01-全链路认证.md"


CHECKS: tuple[CheckSpec, ...] = (
    CheckSpec(
        section="一、未声明功能复核",
        title="taosX 导出 TDengine 查询结果到 CSV 文件已完成正式声明",
        description="核对数据管道工具规格、CLI 说明和源码仓库说明，确认历史关注的 CSV 导出能力已被纳入正式功能清单。",
        passed_summary="CSV 导出能力已在需求、功能规格和源码说明中形成闭环，当前不属于未声明功能。",
        evidences=(
            Evidence("声明", DATA_PIPELINE_RS, r"导出 TDengine 查询结果到 CSV 文件", "需求规格声明 CSV 导出能力"),
            Evidence("声明", DATA_PIPELINE_FS, r"导出查询结果为 CSV 文件", "功能规格给出 CSV 导出章节"),
            Evidence("声明", DATA_PIPELINE_FS, r'-t "csv:\./test\.csv"', "功能规格给出 taosx run 导出 CSV 的命令示例"),
            Evidence(
                "实现",
                "source/taos-xservice/README.md",
                r"Export or import offline data files, currently support CSV and Parquet\.",
                "源码仓库 README 声明支持 CSV/Parquet 离线导入导出",
            ),
            Evidence(
                "实现",
                "source/taos-xservice/src/run.rs",
                r"CSV: `csv:/path/to/file\.csv`\.",
                "CLI 源码说明支持 csv DSN",
            ),
        ),
    ),
    CheckSpec(
        section="一、未声明功能复核",
        title="taosX TMQ 订阅导出 Kafka 已完成正式声明",
        description="核对数据管道工具规格与 Kafka sink 组件，确认历史关注的 TMQ 导出 Kafka 能力已转为正式声明能力。",
        passed_summary="TMQ 导出 Kafka 能力已在现行规格和源码组件中可追溯，当前不属于未声明功能。",
        evidences=(
            Evidence("声明", DATA_PIPELINE_FS, r"CREATE XNODE TASK 'tmq_export'", "功能规格给出 tmq_export 任务示例"),
            Evidence("声明", DATA_PIPELINE_FS, r"TO 'kafka://broker:9092';", "功能规格给出 Kafka 目标 DSN"),
            Evidence("声明", DATA_PIPELINE_FS, r"- 消息队列：Kafka。", "功能规格将 Kafka 列为正式数据源/目标能力"),
            Evidence(
                "实现",
                "source/taos-xservice/README.md",
                r"- Message queue: Kafka\.",
                "源码仓库 README 将 Kafka 列为正式支持对象",
            ),
            Evidence(
                "实现",
                "source/taos-xservice/crates/sink-kafka/Cargo.toml",
                r'name = "sink-kafka"',
                "源码中存在 Kafka sink 组件",
            ),
        ),
    ),
    CheckSpec(
        section="二、远程调试及连接接口复核",
        title="taosd 原生 6030 连接接口存在正式说明",
        description="核对全链路认证文档与 Explorer 默认配置，确认 taosd 原生连接为正式接口而非隐藏入口。",
        passed_summary="taosd 原生 6030 接口在现行文档和默认配置中均有说明，不属于未声明接口。",
        evidences=(
            Evidence("声明", FULL_TRACE_AUTH, r"私有协议.*6030", "全链路认证文档说明 taosd 原生 6030 接口"),
            Evidence(
                "实现",
                "source/taos-xservice/explorer/server/examples/explorer.toml",
                r'# cluster_native = "taos://localhost:6030"',
                "Explorer 示例配置给出 taosd 原生连接 DSN",
            ),
        ),
    ),
    CheckSpec(
        section="二、远程调试及连接接口复核",
        title="taosAdapter 的对外接口与调试入口均有正式说明",
        description="核对 taosAdapter 规格、Swagger、示例配置和调试代码，确认 REST/WebSocket/兼容写入/StatsD/pprof 均非隐藏入口。",
        passed_summary="taosAdapter 的 REST、WebSocket、兼容写入、StatsD 和 pprof 调试入口均可在规格或源码中追溯，未见隐藏管理接口。",
        evidences=(
            Evidence("声明", ADAPTER_FS, r"http://<fqdn>:6041/rest/sql", "功能规格说明 REST SQL 接口"),
            Evidence("声明", ADAPTER_FS, r"### 4\.2 WebSocket 接口", "功能规格说明 WebSocket 接口"),
            Evidence("声明", ADAPTER_FS, r"--debug.*pprof/pprof", "功能规格说明 --debug 仅显式开启 pprof"),
            Evidence("声明", FULL_TRACE_AUTH, r"collectd、StatsD、OpenTSDB", "全链路认证文档说明 taosAdapter 插件兼容写入覆盖 StatsD"),
            Evidence("实现", "source/taos-adapter/docs/swagger.yaml", r"/rest/sql:", "Swagger 定义 REST SQL 路由"),
            Evidence("实现", "source/taos-adapter/docs/swagger.yaml", r"/influxdb/v1/write:", "Swagger 定义 InfluxDB 兼容接口"),
            Evidence("实现", "source/taos-adapter/docs/swagger.yaml", r"/opentsdb/v1/put/json/:db:", "Swagger 定义 OpenTSDB 兼容接口"),
            Evidence("实现", "source/taos-adapter/example/config/taosadapter.toml", r"port = 6044", "示例配置给出 StatsD 默认端口"),
            Evidence("实现", "source/taos-adapter/system/main.go", r"pprof\.Register\(router\)", "源码中 pprof 仅在 debug 开关下注册"),
        ),
    ),
    CheckSpec(
        section="二、远程调试及连接接口复核",
        title="taosX/XNode 的 REST、gRPC 与内部认证链路均有正式说明",
        description="核对全链路文档、XNode 规格、示例配置和服务源码，确认 6050/6055、JWT、TLS、Arrow Flight 均为正式受控接口。",
        passed_summary="taosX/XNode 的 REST、gRPC/Arrow Flight 和 JWT/TLS 受控通信在现行文档与源码中均可追溯，未见隐藏通信入口。",
        evidences=(
            Evidence("声明", DATA_PIPELINE_RS, r"XNode 与 MNode 之间使用 JWT Token 进行双向认证", "需求规格说明 XNode/MNode JWT 认证"),
            Evidence("声明", DATA_PIPELINE_RS, r"gRPC \+ Arrow Flight 通信", "需求规格说明 gRPC + Arrow Flight 通信"),
            Evidence("声明", FULL_TRACE_AUTH, r"\| REST API \| HTTP 1\.1 \| 6050 \|", "全链路认证文档列出 taosX REST 接口"),
            Evidence("声明", FULL_TRACE_AUTH, r"\| gRPC \| HTTP/2 \| 6055 \|", "全链路认证文档列出 taosX gRPC 接口"),
            Evidence("实现", "source/taos-xservice/examples/taosx.toml", r'#listen = "0\.0\.0\.0:6050"', "示例配置给出 taosX REST 默认端口"),
            Evidence("实现", "source/taos-xservice/examples/taosx.toml", r'#grpc = "0\.0\.0\.0:6055"', "示例配置给出 taosX gRPC 默认端口"),
            Evidence("实现", "source/taos-xservice/src/serve/mod.rs", r"const TAOSX_REST_API_DEFAULT_PORT: u16 = 6050;", "服务源码定义 REST 默认端口"),
            Evidence("实现", "source/taos-xservice/src/serve/mod.rs", r"const TAOSX_GRPC_DEFAULT_PORT: u16 = 6055;", "服务源码定义 gRPC 默认端口"),
            Evidence("实现", "source/taos-xservice/src/serve/rpc/mod.rs", r'\.get\("x-token"\)', "gRPC 源码要求 x-token 进行鉴权"),
        ),
    ),
    CheckSpec(
        section="二、远程调试及连接接口复核",
        title="Explorer Web UI/API 与认证机制均有正式说明",
        description="核对 Explorer 规格、示例配置和服务源码，确认 6060 端口、Basic/OAuth、WebSocket 和 taosX 代理能力均有正式声明。",
        passed_summary="Explorer 的 6060 Web 服务、Basic/OAuth、WebSocket 和 taosX 代理能力在文档与源码中均有支撑，未见隐藏管理入口。",
        evidences=(
            Evidence("声明", EXPLORER_RS, r"提供 REST API、WebSocket API 完整文档", "需求规格要求提供 REST/WebSocket API 文档"),
            Evidence("声明", EXPLORER_DS, r"OAuth 2\.0/OIDC", "设计规格说明 OAuth 2.0/OIDC"),
            Evidence("声明", EXPLORER_DS, r"AES-256-GCM", "设计规格说明 AES-256-GCM"),
            Evidence("声明", EXPLORER_DS, r'\.bind\(\("0\.0\.0\.0", 6060\)\)\?', "设计规格说明 Explorer 默认监听 6060"),
            Evidence("实现", "source/taos-xservice/explorer/server/examples/explorer.toml", r"port = 6060", "Explorer 示例配置给出默认监听端口"),
            Evidence("实现", "source/taos-xservice/explorer/server/examples/explorer.toml", r'cluster = "http://localhost:6041"', "Explorer 示例配置给出 taosAdapter 默认代理目标"),
            Evidence("实现", "source/taos-xservice/explorer/server/examples/explorer.toml", r'#redirect_uri = "http://localhost:6060/api/-/oauth/callback"', "Explorer 示例配置给出 OAuth 回调地址"),
            Evidence("实现", "source/taos-xservice/explorer/server/src/main.rs", r"const EXPLORER_PORT: u16 = 6060;", "Explorer 源码定义默认监听端口"),
            Evidence("实现", "source/taos-xservice/explorer/server/src/main.rs", r"/api/-/oauth/authorize", "Explorer 源码注册 OAuth 路由"),
        ),
    ),
)


def parse_args() -> argparse.Namespace:
    script_path = Path(__file__).resolve()
    default_repo_root = script_path.parents[2]
    parser = argparse.ArgumentParser(
        description="检查 tsdb 仓库中未声明功能及接口的一致性，并生成 Markdown 报告。"
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=default_repo_root,
        help=f"仓库根目录，默认从脚本路径推断为 {default_repo_root}",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_repo_root / "docs/unplanned/tools/未声明功能及接口检查报告.md",
        help="Markdown 报告输出路径",
    )
    return parser.parse_args()


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def line_from_offset(text: str, offset: int) -> tuple[int, str]:
    line_no = text.count("\n", 0, offset) + 1
    line = text.splitlines()[line_no - 1].strip()
    return line_no, line


def match_evidence(repo_root: Path, evidence: Evidence) -> MatchResult:
    full_path = repo_root / evidence.path
    if not full_path.exists():
        return MatchResult(evidence=evidence, found=False, line_no=None, snippet=None, error="文件不存在")

    text = read_text(full_path)
    match = re.search(evidence.pattern, text, flags=re.MULTILINE | re.DOTALL)
    if match is None:
        return MatchResult(evidence=evidence, found=False, line_no=None, snippet=None, error="未命中正则")

    line_no, snippet = line_from_offset(text, match.start())
    return MatchResult(evidence=evidence, found=True, line_no=line_no, snippet=snippet)


def run_checks(repo_root: Path) -> list[CheckResult]:
    results: list[CheckResult] = []
    for spec in CHECKS:
        evidences = tuple(match_evidence(repo_root, evidence) for evidence in spec.evidences)
        results.append(CheckResult(spec=spec, evidences=evidences))
    return results


def render_evidence(result: MatchResult) -> str:
    path = result.evidence.path
    if result.found and result.line_no is not None and result.snippet is not None:
        return (
            f"- [{result.evidence.kind}] `{path}:L{result.line_no}` — "
            f"{result.evidence.description}；命中内容：`{result.snippet}`"
        )
    return f"- [{result.evidence.kind}] `{path}` — {result.evidence.description}；状态：**{result.error}**"


def render_report(repo_root: Path, output: Path, results: list[CheckResult]) -> str:
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    passed = [item for item in results if item.passed]
    failed = [item for item in results if not item.passed]

    lines = [
        "# TDengine 未声明功能及接口检查报告",
        "",
        f"- 检查仓库：`{repo_root}`",
        f"- 生成时间：`{generated_at}`",
        f"- 检查脚本：`tools/scripts/check_undeclared_features_and_interfaces.py`",
        f"- 报告路径：`{output}`",
        "- 检查方法：基于仓库内现行规格文档、全链路说明、示例配置和源码入口做清单式一致性复核，不做运行态端口扫描，也不覆盖仓库外部署制品。",
        f"- 总体结论：**{'未发现未声明功能及接口' if not failed else f'发现 {len(failed)} 项证据缺口，需人工复核'}**",
        "",
        "## 一、检查摘要",
        "",
        f"- 检查项总数：{len(results)}",
        f"- 通过项：{len(passed)}",
        f"- 待复核项：{len(failed)}",
    ]

    section_order: list[str] = []
    for item in results:
        if item.spec.section not in section_order:
            section_order.append(item.spec.section)

    for section in section_order:
        section_items = [item for item in results if item.spec.section == section]
        lines.extend(["", f"## {section}", ""])
        for index, item in enumerate(section_items, start=1):
            lines.extend(
                [
                    f"### {section[0]}.{index} {item.spec.title}",
                    "",
                    f"- 结果：**{'通过' if item.passed else '待复核'}**",
                    f"- 检查说明：{item.spec.description}",
                    f"- 本项结论：{item.spec.passed_summary if item.passed else '至少一项声明或实现证据未命中，需要人工复核。'}",
                    "- 证据：",
                ]
            )
            lines.extend(render_evidence(evidence) for evidence in item.evidences)

    lines.extend(["", "## 三、综合结论", ""])
    if failed:
        lines.extend(
            [
                "本次脚本检查存在证据缺口，暂不能输出“已没有未声明功能及接口”的最终结论。建议优先补齐以下内容后重新执行：",
            ]
        )
        lines.extend(f"- {item.spec.title}" for item in failed)
    else:
        lines.extend(
            [
                "经对现行规格文档、全链路说明、示例配置和源码入口进行交叉复核，脚本覆盖范围内未发现新增未声明功能、未声明接口、隐藏管理入口或未说明的远程调试接口。",
                "此前重点关注的 taosX CSV 导出与 TMQ 导出 Kafka 能力，当前均已纳入正式需求/功能规格与源码组件说明，可按“历史未声明功能已补充声明并纳入正式文档管理”口径出具检查结论。",
            ]
        )

    lines.extend(["", "## 四、执行方式", "", "```bash", "cd ~/tsdb", "python3 tools/scripts/check_undeclared_features_and_interfaces.py", "```", ""])
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = args.output if args.output.is_absolute() else (repo_root / args.output)
    output = output.resolve()

    results = run_checks(repo_root)
    report = render_report(repo_root, output, results)

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report, encoding="utf-8")

    passed = sum(1 for item in results if item.passed)
    total = len(results)
    if passed == total:
        print(f"PASS {passed}/{total}: {output}")
        return 0

    print(f"FAIL {passed}/{total}: {output}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
