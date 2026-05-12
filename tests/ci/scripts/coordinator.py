#!/usr/bin/env python3
# =============================================================================
# coordinator.py — 动态测试用例分发协调器
# =============================================================================
# 运行位置: builder (192.168.2.207)，测试阶段与 worker job 并行
#
# 功能:
#   1. 解析 cases.task 并过滤，构建全局用例队列
#   2. HTTP 服务供 worker 拉取用例 / 上报结果
#   3. 每隔 PROM_INTERVAL 秒查询 Prometheus，更新各 worker 负载评分
#   4. 根据负载评分决定分配给 worker 的用例批次大小
#   5. 全部完成后生成聚合 JUnit XML，然后退出
#
# 端口: 23000 + CI_MERGE_REQUEST_IID  (MR#37 → 23037)
#
# 环境变量:
#   CASES_TASK        — cases.task 路径
#   SANITIZER         — y|n (默认 n)
#   RESULTS_DIR       — JUnit 输出目录 (默认 ./results)
#   PROMETHEUS_URL    — Prometheus 地址 (默认 http://192.168.1.42:9090)
#   PROM_INTERVAL     — Prometheus 查询间隔秒数 (默认 30)
#   CI_MERGE_REQUEST_IID — MR 号，决定端口
#   COORDINATOR_PORT  — 手动指定端口（优先于 MR 计算）
#   MAX_WAIT_SECONDS  — 无 worker 心跳后最长等待 (默认 600)
#   RERUN_MODE        — 重跑模式（auto=自动检测, 空=全量, failed=仅失败, worker:HOST, failed:HOST）
# =============================================================================

import argparse
import json
import os
import queue
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer

# ── 配置 ──────────────────────────────────────────────────────────────────────
PROMETHEUS_URL  = os.environ.get("PROMETHEUS_URL",  "http://192.168.1.42:9090")
PROM_INTERVAL   = int(os.environ.get("PROM_INTERVAL", "30"))
SANITIZER       = os.environ.get("SANITIZER", "n")
MAX_WAIT        = int(os.environ.get("MAX_WAIT_SECONDS", "1200"))   # 无进度最长等待：允许 worker 处理大批量
RESULTS_DIR     = os.environ.get("RESULTS_DIR", "./results")
# 单个用例最长运行时间（仅对心跳已超时的 worker 生效）：
# 如果 worker 心跳正常，说明它还在执行任务，不应按 assigned_at 超时
# 只有当 worker 心跳也超时时，才按 assigned_at 判定用例超时
MAX_CASE_TIMEOUT = int(os.environ.get("MAX_CASE_TIMEOUT", "1800"))  # 默认 30 分钟（大批量串行执行需要）
# Worker 心跳超时：超过此时间未收到心跳的 worker 视为死亡，其 in_flight 用例立即收割
# Worker 每 30s 发一次心跳，120s 超时 = 容忍 4 次心跳丢失
WORKER_HEARTBEAT_TIMEOUT = int(os.environ.get("WORKER_HEARTBEAT_TIMEOUT", "120"))
# 端口算法: CI_PIPELINE_ID % 10000 + 20000  (20000-29999)
# • CI_PIPELINE_ID 在同一 pipeline 内所有 job 相同，不需额外传参
# • 适用于 MR / schedule / 手动触发，无一例外
# • COORDINATOR_PORT 环境变量可手动覆盖
_pipeline_id = int(os.environ.get("CI_PIPELINE_ID", "0") or "0")
DEFAULT_PORT = int(os.environ.get("COORDINATOR_PORT",
                                   str(_pipeline_id % 10000 + 20000)))

# ── Rerun 模式 ────────────────────────────────────────────────────────────────
# RERUN_MODE 控制重跑行为（需配合 STATE_DIR 中的 results.json）：
#   "auto"（默认）    — 自动检测：存在上次结果时仅重跑失败 case，否则全量
#   空               — 强制全量跑所有 case
#   "failed"         — 仅重跑上次失败的 case
#   "worker:HOST"    — 仅重跑上次分配给 HOST 的所有 case
#   "failed:HOST"    — 仅重跑上次分配给 HOST 且失败的 case
# 在 .gitlab-ci.yml 的 coordinator.variables 中设置默认值，无需额外权限。
# Retry 时自动生效，无需手动干预。
RERUN_MODE = os.environ.get("RERUN_MODE", "auto").strip()
# 持久化状态目录：每次运行结束保存 results.json，rerun 时读取
_CI_BASE_DIR = os.environ.get("CI_BASE_DIR", "/data1/tdengine-ci")
STATE_DIR = os.path.join(_CI_BASE_DIR, "coordinator-state", f"pipeline-{_pipeline_id}")

# ── 全局状态（线程安全） ───────────────────────────────────────────────────────
_lock          = threading.Lock()

# ── 日志过滤辅助 ──────────────────────────────────────────────────────────────
_PIP_NOISE_PREFIXES = (
    "Collecting ", "Downloading ", "  Downloading ",
    "Requirement already", "Successfully installed", "Successfully uninstalled",
    "Attempting uninstall", "Found existing installation", "Uninstalling ",
    "WARNING: Running pip", "[notice]", "-----",
    "Looking in indexes",
)
def _filter_pip_noise(text: str) -> str:
    """过滤 pip 安装噪音行，只保留关键内容。"""
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if any(stripped.startswith(p) for p in _PIP_NOISE_PREFIXES):
            continue
        # 过滤 pip 进度条（━━━ 符号）
        if stripped.startswith("\u2501") or all(c in "\u2501 " for c in stripped):
            continue
        lines.append(line)
    return "\n".join(lines)


# 失败用例本地保留基础路径（runner 本地，不被 after_script 清理）
_FAIL_RETAIN_BASE = "/data1/tdengine-ci/fail-logs"
# HTTP 文件服务端口
_FAIL_HTTP_PORT = 8899

def _make_slug(cmd: str) -> str:
    """从 cmd 字符串中提取用例标识部分并转换为 slug（不含 nN- 前缀）。
    优先匹配 cases/...，fallback 到最后一个路径参数（如 82-UnitTest/test.sh）。
    与 run-test-dynamic.sh 中的 slug 生成逻辑一致（去掉 n${NODE_INDEX}- 前缀）。"""
    import re as _re
    m = _re.search(r'cases/\S+', cmd)
    if m:
        raw = m.group(0)
        slug = raw.replace('cases/', '', 1)
    else:
        # fallback: 取 cmd 中最后一个含 / 的 token（如 82-UnitTest/test.sh）
        tokens = cmd.split()
        path_tokens = [t for t in tokens if '/' in t and not t.startswith('-') and not t.startswith('http')]
        if not path_tokens:
            return ""
        raw = path_tokens[-1]
        slug = raw
    slug = slug.replace('/', '__')
    slug = slug.replace('.py', '').replace('.sh', '')
    slug = _re.sub(r'[\[\*\?]', '_', slug)
    slug = _re.sub(r'[^A-Za-z0-9_.-]', '_', slug)
    slug = _re.sub(r'_+', '_', slug)
    slug = slug.strip('_')
    return slug

def _print_fail_sections(sec_id: str, sec_title: str, log_b64: str,
                         ts: int = 0, label: str = "",
                         browse_url: str = "") -> None:
    """打印单个折叠 GitLab section：过滤后的错误摘要。
    完整日志请查看 artifacts 中对应用例目录下的 case.txt，
    或通过 browse_url 直接在 runner 上浏览。"""
    import base64 as _b64
    if ts == 0:
        ts = int(time.time())
    full_text = ""
    if log_b64:
        try:
            full_text = _b64.b64decode(log_b64).decode("utf-8", "replace")
        except Exception as e:
            full_text = f"(log decode error: {e})"

    print(f"\x1b[0Ksection_start:{ts}:{sec_id}[collapsed=true]\r\x1b[0K\x1b[31;1m{sec_title}\x1b[0m")
    print("\u2500" * 64)
    if browse_url:
        print(f"Runner logs: {browse_url}")
    if full_text:
        filtered = _filter_pip_noise(full_text)
        flines = filtered.splitlines()
        # 找到最后一个关键错误锚点（pytest 汇总 / AssertionError / Error: / FAILED / exit code）
        ANCHORS = ("short test summary", "AssertionError", "AssertionError",
                   "Error:", "FAILED ", "Execute script failure",
                   "Traceback (most recent", "TSIM ERROR", "system error")
        anchor_idx = -1
        for i, ln in enumerate(flines):
            if any(a in ln for a in ANCHORS):
                anchor_idx = i
        if anchor_idx >= 0:
            # 从锚点往前 10 行，最多 80 行
            start = max(0, anchor_idx - 10)
            summary_lines = flines[start:start + 80]
        else:
            # 无锚点：取最后 80 行
            summary_lines = flines[-80:]
        print("\n".join(summary_lines) if summary_lines else "(no key error lines found)")
    else:
        if browse_url:
            # TIMEOUT/LOST: 无日志但有 browse URL，提示用户浏览目录
            _hint = _make_slug(label) if label else ""
            if _hint:
                print(f"(no log captured — browse directory listing above, look for *{_hint}*)")
            else:
                print("(no log captured — browse directory listing above)")
        else:
            print("(no log captured — check worker artifact results/logs/)")
    print("\u2500" * 64)
    print(f"\x1b[0Ksection_end:{ts}:{sec_id}\r\x1b[0K")
# 普通用例队列（无 caps 要求，任意 worker 可执行）
_queue_normal  = []          # list of (idx, path, cmd, runner)
_pos_normal    = 0
# Large-Mem 专属队列（priority 字段为整数，须在 large-mem worker 上运行）
_queue_largemem = []         # list of (idx, path, cmd, runner)
_pos_largemem  = 0
_in_flight     = {}          # case_idx → {worker, assigned_at}
_results       = []          # [{idx, worker, path, cmd, rc, elapsed_ms, worker_ip, ...}]
_worker_scores  = {}          # worker_hostname → load_score (0-100, 越低越空闲)
_worker_ncpus   = {}          # worker_hostname → logical CPU count (from Prometheus)
_worker_ips     = {}          # worker_hostname → prometheus instance (ip:9100)
_worker_last    = {}          # worker_hostname → last_seen timestamp
_worker_new     = set()       # 还未完成首次 Prometheus 查询的 worker
_prom_miss_cnt  = {}          # instance → 连续 miss 次数，用于抜制重复告警
_total_cases   = 0
_start_time    = time.time()

# ── Prometheus 查询 ───────────────────────────────────────────────────────────
def prom_query(promql: str) -> float | None:
    url = f"{PROMETHEUS_URL}/api/v1/query?query={urllib.parse.quote(promql)}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            d = json.loads(resp.read())
        results = d.get("data", {}).get("result", [])
        if results:
            return float(results[0]["value"][1])
    except Exception:
        pass
    return None

def get_load_score(instance: str) -> tuple[float, int]:
    """
    综合负载评分 (0-100)，越低越空闲，同时返回 CPU 核数。

    公式: cpu_busy*0.50 + iowait*0.30 + load_ratio*0.20

    设计依据:
    - cpu_busy  : [5m] 窗口平滑毛刺，CPU 是 CI 测试的主要瓶颈，权重最高 (0.50)
    - iowait    : [5m] 窗口，磁盘 IO 次于 CPU，权重 0.30
    - load_ratio: (load1+load5)/2 / ncpus，兼顾短期响应和中期趋势，
                  已按核数归一化，权重 0.20；三者优先级 cpu > io > load，
                  io 与 load 相差 0.10 保持接近
    """
    cpu_busy = prom_query(
        f'100 - (avg by(instance)(rate(node_cpu_seconds_total{{mode="idle",'
        f'instance="{instance}"}}[5m])) * 100)'
    )
    iowait = prom_query(
        f'avg by(instance)(rate(node_cpu_seconds_total{{mode="iowait",'
        f'instance="{instance}"}}[5m])) * 100'
    )
    load1 = prom_query(f'node_load1{{instance="{instance}"}}')
    load5 = prom_query(f'node_load5{{instance="{instance}"}}')
    ncpus = prom_query(
        f'count(node_cpu_seconds_total{{mode="idle",instance="{instance}"}})'
    )
    if None in (cpu_busy, iowait, load1, load5, ncpus) or ncpus == 0:
        cnt = _prom_miss_cnt.get(instance, 0) + 1
        _prom_miss_cnt[instance] = cnt
        # 只在第 1、3、1021、每 60 次打一次告警，防止每30s就刷屏
        if cnt == 1 or cnt == 3 or cnt % 60 == 0:
            print(f"[coordinator] WARN: Prometheus miss for {instance} (x{cnt}), using score=50")
        return 50.0, 0
    _prom_miss_cnt[instance] = 0  # 恢复后清零
    # 混合 1分钟 和 5分钟 load average，兼顾响应速度和平稳性
    load_blend = (load1 + load5) / 2
    load_ratio = min((load_blend / ncpus) * 100, 100)
    score = cpu_busy * 0.50 + iowait * 0.30 + load_ratio * 0.20
    return round(score, 1), int(ncpus)

def prometheus_refresh_loop():
    """后台线程：定期刷新所有已注册 worker 的负载评分"""
    while True:
        time.sleep(PROM_INTERVAL)
        with _lock:
            workers = dict(_worker_ips)
        for hostname, instance in workers.items():
            score, ncpus = get_load_score(instance)
            with _lock:
                _worker_scores[hostname] = score
                if ncpus > 0:
                    _worker_ncpus[hostname] = ncpus
            ncpus_str = f"{ncpus}c" if ncpus > 0 else "?c"
            print(f"[coordinator] load score  {hostname}({instance}) = {score:.1f}%  [{ncpus_str}]")

# ── 用例解析 ──────────────────────────────────────────────────────────────────
def load_cases(cases_task_path: str, sanitizer: str, path_filter: str = "") -> list:
    """
    返回 list of (path, cmd, required_caps)。
    required_caps: 空字符串表示无要求；"large-mem" 表示需要大内存 worker。
    判断依据：第一列（priority）为整数值 → required_caps = "large-mem"。
    """
    cases = []
    with open(cases_task_path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = line.split(",", 4)
            if len(parts) < 5:
                continue
            priority = parts[0].strip()
            san  = parts[2].strip()
            path = parts[3].strip()
            cmd  = parts[4].strip()
            if not path or not cmd:
                continue
            # SANITIZER=y → 只跑 san=y；SANITIZER=n → 全部跑
            if sanitizer == "y" and san != "y":
                continue
            # path_filter 非空时只加载匹配的路径类型
            if path_filter and path != path_filter:
                continue
            # priority 字段为整数 → 需要 large-mem worker
            required_caps = "large-mem" if priority.isdigit() else ""
            cases.append((path, cmd, required_caps))
    return cases

# ── 分配策略 ──────────────────────────────────────────────────────────────────
# 基准 CPU 核数：低于此值不放大批量，高于此值按比例适度放大
_NCPUS_BASELINE = 8

def calc_batch_size(worker: str, requested: int) -> int:
    """
    根据 worker 负载评分和 CPU 核数决定实际分配数量。

    score 越低越空闲；当 score < 40% 时允许突破 requested 上限，
    直接按核数放大（上限 ncpus）以充分利用空闲资源。

    阈值设计（保守策略，适配多 MR 并发）：
      - score >= 70% 时停止分配，为其它 MR 留余量
      - score < 30% 时允许适度放大，但上限为 ncpus/2 而非 ncpus
      - 整体更保守，避免多 MR 并发时用例大批量超时
    """
    score = _worker_scores.get(worker, 50.0)
    ncpus = _worker_ncpus.get(worker, _NCPUS_BASELINE)
    # 核数放大系数：保守模式，上限 1.5×
    cpu_factor = min(1.5, 1.0 + 0.3 * (ncpus / _NCPUS_BASELINE - 1))

    if score >= 70:
        return 0           # 负载较高时完全停止分配
    elif score >= 55:
        cap = max(1, int(requested * 0.30 * cpu_factor))
    elif score >= 40:
        cap = max(1, int(requested * 0.50 * cpu_factor))
    elif score >= 30:
        cap = max(1, int(requested * 0.75 * cpu_factor))
    else:
        # 空闲时适度放大，上限 ncpus/2（不吃满资源）
        cap = max(requested, int(ncpus * cpu_factor / 2))
    return cap

# ── HTTP 请求处理 ──────────────────────────────────────────────────────────────
class ReusableHTTPServer(HTTPServer):
    """允许快速重启时复用 TIME_WAIT 状态的端口"""
    allow_reuse_address = True


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        # 只打印错误，减少 noise
        if args and str(args[1]) not in ("200", "204"):
            print(f"[coordinator] HTTP {args[1]}  {args[0]}")

    def send_json(self, code: int, obj: dict):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        # _queue_pos is assigned (+=) inside /api/next, so Python would treat it
        # as local throughout the entire method without this declaration.
        global _queue_pos

        parsed = urllib.parse.urlparse(self.path)
        params = dict(urllib.parse.parse_qsl(parsed.query))
        path   = parsed.path

        # ── /api/next?worker=u3-141&ip=192.168.3.141&slots=3 ──────────────────
        if path == "/api/next":
            global _pos_normal, _pos_largemem
            worker   = params.get("worker", "unknown")
            ip       = params.get("ip", "")
            slots    = int(params.get("slots", "1"))
            # caps: 逗号分隔的能力列表，如 "large-mem" 或 "" (普通 worker)
            worker_caps = set(c.strip() for c in params.get("caps", "").split(",") if c.strip())
            is_largemem_worker = "large-mem" in worker_caps

            with _lock:
                now = time.time()
                _worker_last[worker] = now
                is_new_worker = False
                if ip and worker not in _worker_ips:
                    instance = f"{ip}:9100"
                    _worker_ips[worker] = instance
                    _worker_new.add(worker)
                    # 首次注册立即查询一次（后台线程）
                    def _init_score(w=worker, inst=instance):
                        s, n = get_load_score(inst)
                        with _lock:
                            _worker_scores[w] = s
                            if n > 0:
                                _worker_ncpus[w] = n
                            _worker_new.discard(w)
                    threading.Thread(target=_init_score, daemon=True).start()
                    is_new_worker = True

                # 可用 case 数：large-mem worker 可取两个队列，普通 worker 只取 normal
                remaining_lm  = len(_queue_largemem) - _pos_largemem
                remaining_nor = len(_queue_normal)   - _pos_normal
                if is_largemem_worker:
                    remaining = remaining_lm + remaining_nor
                else:
                    remaining = remaining_nor

                # 首次 poll 时 Prometheus 评分还未就绪，先发小批量探针包
                if is_new_worker or worker in _worker_new:
                    allocated = min(2, slots, remaining)
                else:
                    # score < 60% 时允许 prefetch：按「空闲余量」计算可预取数。
                    # prefetch 上限为 ncpus/2，避免多 MR 并发时吃满资源。
                    #
                    # 示例：u0-210  score=24%  ncpus=24  slots=1
                    #   headroom = (60-24)/60 = 0.60
                    #   max_prefetch = max(4, 24//2) = 12
                    #   prefetch = min(int(0.60*12), 12) = 7
                    #   slot_cap = 1+7 = 8
                    #   calc_batch_size(worker, 8) → score<30 → cap=max(8,int(24*1.2/2))=14
                    #   allocated = min(14, remaining, 8) = 8  ✓
                    #
                    # 多 MR 并行时 score 升至 40-60%+，headroom 缩小或归零，
                    # prefetch 显著减少，天然退化到保守模式。
                    worker_score_now = _worker_scores.get(worker, 50.0)
                    if worker_score_now < 60:
                        ncpus_now = _worker_ncpus.get(worker, _NCPUS_BASELINE)
                        headroom = (60.0 - worker_score_now) / 60.0
                        # prefetch 上限：ncpus/2，避免多 MR 并发时分配过多
                        max_prefetch = max(4, ncpus_now // 2)
                        prefetch = min(int(headroom * ncpus_now / 2), max_prefetch)
                        slot_cap = slots + prefetch
                    else:
                        slot_cap = slots
                    allocated = calc_batch_size(worker, slot_cap)
                    allocated = min(allocated, remaining, slot_cap)

                assigned = []
                count = 0
                # Large-Mem worker: 优先消耗 largemem 队列
                if is_largemem_worker:
                    while count < allocated and _pos_largemem < len(_queue_largemem):
                        idx, case_path, case_cmd, case_runner = _queue_largemem[_pos_largemem]
                        _pos_largemem += 1
                        _in_flight[idx] = {"worker": worker, "assigned_at": now}
                        assigned.append({"idx": idx, "path": case_path, "cmd": case_cmd, "runner": case_runner})
                        count += 1
                # 所有 worker: 从 normal 队列取剩余
                while count < allocated and _pos_normal < len(_queue_normal):
                    idx, case_path, case_cmd, case_runner = _queue_normal[_pos_normal]
                    _pos_normal += 1
                    _in_flight[idx] = {"worker": worker, "assigned_at": now}
                    assigned.append({"idx": idx, "path": case_path, "cmd": case_cmd, "runner": case_runner})
                    count += 1

                score      = _worker_scores.get(worker, 50.0)
                queue_left = (len(_queue_normal) - _pos_normal)
                if is_largemem_worker:
                    queue_left += (len(_queue_largemem) - _pos_largemem)
                all_done   = (_pos_normal  >= len(_queue_normal) and
                              _pos_largemem >= len(_queue_largemem) and
                              len(_in_flight) == 0)

            caps_str = f"caps={','.join(sorted(worker_caps)) or 'none'}"
            print(
                f"[coordinator] → {worker:10s} [{caps_str}] score={score:.0f}%"
                f"  assigned={len(assigned)}  queue_left={queue_left}"
            )
            self.send_json(200, {
                "cases":      assigned,
                "queue_left": queue_left,
                "all_done":   all_done,
                "wait_ms":    5000 if allocated == 0 and remaining > 0 else 0,
            })
            return

        # ── /api/status ────────────────────────────────────────────────────────
        if path == "/api/status":
            with _lock:
                done  = len(_results)
                inflt = len(_in_flight)
                left  = (len(_queue_normal) - _pos_normal) + (len(_queue_largemem) - _pos_largemem)
                lm_left = len(_queue_largemem) - _pos_largemem
                wscores = dict(_worker_scores)
                elapsed = int(time.time() - _start_time)
            self.send_json(200, {
                "total":           _total_cases,
                "done":            done,
                "in_flight":       inflt,
                "queue_left":      left,
                "queue_largemem":  lm_left,
                "elapsed_s":       elapsed,
                "worker_scores":   wscores,
            })
            return

        # ── /api/heartbeat?worker=xxx ────────────────────────────────────────
        if path == "/api/heartbeat":
            worker = params.get("worker", "unknown")
            with _lock:
                _worker_last[worker] = time.time()
            self.send_json(200, {"ok": True})
            return

        self.send_json(404, {"error": "not found"})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path

        # ── /api/done ──────────────────────────────────────────────────────────
        if path == "/api/done":
            length  = int(self.headers.get("Content-Length", 0))
            body    = json.loads(self.rfile.read(length))
            idx     = body.get("idx")
            rc      = int(body.get("rc", 0))
            elapsed = int(body.get("elapsed_ms", 0))
            worker  = body.get("worker", "?")
            cpath   = body.get("path", "?")
            cmd     = body.get("cmd", "?")
            log_b64 = body.get("log_b64", "")   # 失败时的日志 base64

            status = "PASS" if rc == 0 else f"FAIL(exit={rc})"
            print(
                f"[coordinator] ← {worker:10s} [{status}]"
                f"  {cpath}  ({elapsed/1000:.1f}s)"
            )
            with _lock:
                _worker_last[worker] = time.time()
                _in_flight.pop(idx, None)
                # 若该 idx 已被 MAX_CASE_TIMEOUT 收割标为 LOST/TIMEOUT（rc<0），
                # 以 worker 实际结果覆盖（worker 跑完了就是真实结果）
                existing_idx = next(
                    (i for i, r in enumerate(_results) if r["idx"] == idx), None
                )
                worker_ip  = body.get("worker_ip", "")
                slug       = body.get("slug", "")
                new_entry = {
                    "idx": idx, "worker": worker,
                    "path": cpath, "cmd": cmd,
                    "rc": rc, "elapsed_ms": elapsed,
                    "log_b64": log_b64,
                    "worker_ip": worker_ip,
                    "slug": slug,
                }
                if existing_idx is not None:
                    old_rc = _results[existing_idx]["rc"]
                    if old_rc < 0:   # 已被误标为 LOST/TIMEOUT
                        print(f"[coordinator]   OVERWRITE idx={idx} was rc={old_rc} now rc={rc} ({worker})")
                        _results[existing_idx] = new_entry
                    # 否则保留已有结果（重复提交时忽略）
                else:
                    _results.append(new_entry)
            self.send_json(204, {})
            return

        # ── /api/fail  实时失败通知（worker 失败时立即推送，协调器日志实时可见）──
        if path == "/api/fail":
            import base64 as _b64
            length  = int(self.headers.get("Content-Length", 0))
            body    = json.loads(self.rfile.read(length))
            rc      = int(body.get("rc", 0))
            elapsed = int(body.get("elapsed_ms", 0))
            worker  = body.get("worker", "?")
            cpath   = body.get("path", "?")
            cmd     = body.get("cmd", "?")
            log_b64 = body.get("log_b64", "")
            worker_ip = body.get("worker_ip", "")
            slug      = body.get("slug", "")
            label   = f"{cpath}::{cmd}"
            sec_id  = f"fail_rt_{worker}_{int(time.time()*1000)}"
            sec_title = f"FAIL [exit={rc}] [{worker}] {label}  ({elapsed/1000:.1f}s)"
            browse_url = ""
            if worker_ip and slug:
                browse_url = f"http://{worker_ip}:{_FAIL_HTTP_PORT}/{slug}/"
            ts = int(time.time())
            _print_fail_sections(sec_id, sec_title, log_b64, ts=ts, label=label,
                                 browse_url=browse_url)
            # flush 确保 GitLab 日志实时刷新
            sys.stdout.flush()
            self.send_json(204, {})
            return

        self.send_json(404, {"error": "not found"})

# ── JUnit XML 生成 ────────────────────────────────────────────────────────────
import re as _re
_XML_ILLEGAL = _re.compile(
    r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f'          # C0 控制字符（\t\n\r 除外）
    r'\ud800-\udfff'                               # surrogate pairs
    r'\ufffe\uffff]'                               # BOM / nonchar
)
def _xml_safe(text: str) -> str:
    """移除 XML 1.0 非法字符，确保 ElementTree 生成合法文档。"""
    return _XML_ILLEGAL.sub('\ufffd', text)        # 替换为 replacement char

def write_junit(results: list, output_path: str):
    import base64
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    total   = len(results)
    fails   = sum(1 for r in results if r["rc"] != 0)
    elapsed = int(time.time() - _start_time)

    suite = ET.Element("testsuite",
        name="test-linux-aggregate",
        tests=str(total), failures=str(fails),
        errors="0", skipped="0", time=str(elapsed))

    for r in results:
        elapsed_s = f"{r['elapsed_ms']//1000}.{r['elapsed_ms']%1000:03d}"
        label    = f"{r['path']}::{r['cmd']}"
        safe_name = label.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
        safe_path = r['path'].replace("&","&amp;")
        tc = ET.SubElement(suite, "testcase",
            name=safe_name, classname=safe_path,
            time=elapsed_s)
        if r["rc"] != 0:
            failure = ET.SubElement(tc, "failure",
                message=f"exit code {r['rc']} on {r['worker']}")
            if r.get("log_b64"):
                try:
                    raw = base64.b64decode(r["log_b64"]).decode("utf-8", "replace")[-4096:]
                    failure.text = _xml_safe(raw)
                except Exception:
                    pass

    tree = ET.ElementTree(suite)
    ET.indent(tree, space="  ")
    with open(output_path, "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True)
    print(f"[coordinator] JUnit written → {output_path}  ({total} cases, {fails} failures)")


def _save_state(results: list):
    """保存 results.json 到持久化目录，供 rerun 使用。"""
    os.makedirs(STATE_DIR, exist_ok=True)
    state_path = os.path.join(STATE_DIR, "results.json")
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"[coordinator] State saved → {state_path}  ({len(results)} results)")


def _load_previous_results() -> list:
    """加载上次运行的 results.json。"""
    state_path = os.path.join(STATE_DIR, "results.json")
    if not os.path.isfile(state_path):
        return []
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[coordinator] WARN: failed to load previous results: {e}")
        return []


def _apply_rerun_filter(all_cases: list, rerun_mode: str, prev_results: list) -> list:
    """
    根据 RERUN_MODE 和上次结果过滤用例列表。

    all_cases: [(path, cmd, required_caps, runner), ...]
    rerun_mode: "failed" | "worker:HOST" | "failed:HOST"
    prev_results: [{"idx":..., "worker":..., "rc":..., "path":..., "cmd":...}, ...]

    返回过滤后的 [(path, cmd, required_caps, runner), ...] 列表。
    """
    if not prev_results:
        print(f"[coordinator] RERUN_MODE={rerun_mode} but no previous results found, "
              f"falling back to full run")
        return all_cases

    # 建立 (path, cmd) → result 映射
    prev_map = {}
    for r in prev_results:
        key = (r.get("path", ""), r.get("cmd", ""))
        prev_map[key] = r

    filtered = []
    if rerun_mode == "failed":
        for case in all_cases:
            path, cmd = case[0], case[1]
            prev = prev_map.get((path, cmd))
            if prev and prev.get("rc", 0) != 0:
                filtered.append(case)
        print(f"[coordinator] RERUN_MODE=failed: {len(filtered)} failed cases "
              f"(out of {len(all_cases)} total, {len(prev_results)} previous results)")

    elif rerun_mode.startswith("worker:"):
        target_worker = rerun_mode[len("worker:"):]
        for case in all_cases:
            path, cmd = case[0], case[1]
            prev = prev_map.get((path, cmd))
            if prev and prev.get("worker", "") == target_worker:
                filtered.append(case)
        print(f"[coordinator] RERUN_MODE=worker:{target_worker}: {len(filtered)} cases "
              f"previously assigned to {target_worker}")

    elif rerun_mode.startswith("failed:"):
        target_worker = rerun_mode[len("failed:"):]
        for case in all_cases:
            path, cmd = case[0], case[1]
            prev = prev_map.get((path, cmd))
            if prev and prev.get("worker", "") == target_worker and prev.get("rc", 0) != 0:
                filtered.append(case)
        print(f"[coordinator] RERUN_MODE=failed:{target_worker}: {len(filtered)} failed cases "
              f"from {target_worker}")

    else:
        print(f"[coordinator] WARN: unknown RERUN_MODE={rerun_mode}, falling back to full run")
        return all_cases

    if not filtered:
        print(f"[coordinator] WARN: rerun filter matched 0 cases, falling back to full run")
        return all_cases

    return filtered

# ── 主逻辑 ────────────────────────────────────────────────────────────────────
def main():
    global _queue_normal, _queue_largemem, _pos_normal, _pos_largemem, _total_cases

    cases_task = os.environ.get("CASES_TASK", "")
    if not cases_task or not os.path.isfile(cases_task):
        # 尝试从标准路径找
        for candidate in [
            "source/taos-community/test/ci/cases.task",
            "/root/gitlab-runner/builds/*/0/rd-public/tsdb/source/taos-community/test/ci/cases.task",
        ]:
            import glob
            matches = glob.glob(candidate)
            if matches:
                cases_task = matches[0]
                break
    if not cases_task or not os.path.isfile(cases_task):
        print(f"ERROR: cases.task not found. Set CASES_TASK env var.", file=sys.stderr)
        sys.exit(1)

    raw = load_cases(cases_task, SANITIZER)
    # test/ci/cases.task 使用 newfw runner
    main_cases = [(p, c, rcaps, "newfw") for p, c, rcaps in raw]

    # ── Rerun 模式过滤 ────────────────────────────────────────────────────────
    effective_rerun = RERUN_MODE
    if effective_rerun == "auto":
        prev_results = _load_previous_results()
        if prev_results:
            # auto 模式：有上次结果 → 仅重跑失败 case
            effective_rerun = "failed"
            print(f"[coordinator] RERUN_MODE=auto: found {len(prev_results)} previous results, "
                  f"switching to 'failed' mode")
        else:
            # auto 模式：无上次结果 → 全量
            effective_rerun = ""
            print(f"[coordinator] RERUN_MODE=auto: no previous results found, running full suite")
    else:
        prev_results = _load_previous_results() if effective_rerun else []

    if effective_rerun:
        main_cases = _apply_rerun_filter(main_cases, effective_rerun, prev_results)

    # 按 required_caps 分流到两条队列
    idx = 0
    norm_list = []
    lm_list   = []
    for p, c, rcaps, runner in main_cases:
        entry = (idx, p, c, runner)
        if rcaps == "large-mem":
            lm_list.append(entry)
        else:
            norm_list.append(entry)
        idx += 1
    _queue_normal   = norm_list
    _queue_largemem = lm_list
    _total_cases    = len(_queue_normal) + len(_queue_largemem)

    print(f"[coordinator] Loaded {_total_cases} cases total "
          f"(normal={len(norm_list)}, large-mem={len(lm_list)}, "
          f"legacy={sum(1 for _,_,_,r in main_cases if r=='legacy')}, "
          f"newfw={sum(1 for _,_,_,r in main_cases if r=='newfw')}, "
          f"sanitizer={SANITIZER})"
          f" from {cases_task}")
    print(f"[coordinator] Prometheus: {PROMETHEUS_URL}  refresh every {PROM_INTERVAL}s")
    print(f"[coordinator] Listening on 0.0.0.0:{DEFAULT_PORT}")
    print(f"[coordinator] Results dir: {RESULTS_DIR}")
    print(f"[coordinator] State dir: {STATE_DIR}")
    if effective_rerun:
        print(f"[coordinator] *** RERUN_MODE: {RERUN_MODE} → effective: {effective_rerun} ***")
    else:
        print(f"[coordinator] RERUN_MODE: {RERUN_MODE or '(empty)'} — full run")

    os.makedirs(RESULTS_DIR, exist_ok=True)

    # 清理占用端口的僵尸进程（上次 pipeline 未正常退出时残留）
    try:
        import subprocess
        result = subprocess.run(
            ["fuser", "-k", f"{DEFAULT_PORT}/tcp"],
            capture_output=True, timeout=5
        )
        if result.returncode == 0:
            print(f"[coordinator] Killed stale process on port {DEFAULT_PORT}")
            time.sleep(1)
    except Exception:
        pass

    # 启动 Prometheus 刷新后台线程
    t = threading.Thread(target=prometheus_refresh_loop, daemon=True)
    t.start()

    # 启动 HTTP 服务（后台线程）
    server = ReusableHTTPServer(("0.0.0.0", DEFAULT_PORT), Handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    # ── 等待所有用例完成 ───────────────────────────────────────────────────────
    last_progress = time.time()
    done_prev = 0
    last_reap_check = time.time()
    while True:
        time.sleep(5)
        now = time.time()

        # ── 定期检查 in_flight 超时（每 30s 一次）─────────────────────────────
        if now - last_reap_check >= 30:
            last_reap_check = now

            # ── 先判定哪些 worker 心跳已超时（死亡 worker）──────────────────
            with _lock:
                alive_workers = set()
                dead_workers = set()
                for w, last_ts in _worker_last.items():
                    if now - last_ts > WORKER_HEARTBEAT_TIMEOUT:
                        # 检查该 worker 是否还有 in_flight 任务
                        w_inflight = [idx for idx, info in _in_flight.items()
                                      if info["worker"] == w]
                        if w_inflight:
                            dead_workers.add(w)
                    else:
                        alive_workers.add(w)

                # ── 收割死亡 worker 的 in_flight case ──────────────────────
                dead_reap = [
                    (idx, info) for idx, info in _in_flight.items()
                    if info["worker"] in dead_workers
                ]

                # ── 仅对心跳正常的 worker 做 MAX_CASE_TIMEOUT 检查 ─────────
                # 如果 worker 心跳正常，说明它还在串行执行批次中的 case，
                # 不应按 assigned_at 超时。只有超过 MAX_CASE_TIMEOUT 的
                # 活 worker case 才需要关注（避免真正的单用例卡死）
                alive_timed_out = [
                    (idx, info) for idx, info in _in_flight.items()
                    if info["worker"] in alive_workers
                    and now - info["assigned_at"] > MAX_CASE_TIMEOUT
                ]

            # ── 处理死亡 worker 收割 ──────────────────────────────────────
            if dead_reap:
                print(f"[coordinator] WARN: {len(dead_reap)} in_flight case(s) on "
                      f"{len(dead_workers)} dead worker(s) (heartbeat timeout "
                      f"{WORKER_HEARTBEAT_TIMEOUT}s), reaping")
                for idx, info in dead_reap:
                    c_path, c_cmd, c_runner = "", "", "legacy"
                    _all_entries = _queue_normal + _queue_largemem
                    for entry in _all_entries:
                        if entry[0] == idx:
                            _, c_path, c_cmd, c_runner = entry
                            break
                    elapsed_s = int(now - info["assigned_at"])
                    with _lock:
                        _in_flight.pop(idx, None)
                        _wip = _worker_ips.get(info["worker"], "").split(":")[0]
                        _results.append({
                            "idx": idx, "worker": info["worker"],
                            "path": c_path, "cmd": c_cmd,
                            "rc": -1, "elapsed_ms": elapsed_s * 1000,
                            "log_b64": "", "runner": c_runner,
                            "worker_ip": _wip, "slug": "",
                        })
                    print(f"[coordinator]   DEAD-WORKER-REAP idx={idx}  "
                          f"worker={info['worker']}  {c_path}  (no heartbeat for {elapsed_s}s)")
                last_progress = now

            # ── 处理活 worker 的 MAX_CASE_TIMEOUT（真正卡死的 case）─────────
            if alive_timed_out:
                print(f"[coordinator] WARN: {len(alive_timed_out)} in_flight case(s) exceeded "
                      f"{MAX_CASE_TIMEOUT}s on alive worker(s), marking as TIMEOUT")
                for idx, info in alive_timed_out:
                    c_path, c_cmd, c_runner = "", "", "legacy"
                    _all_entries = _queue_normal + _queue_largemem
                    for entry in _all_entries:
                        if entry[0] == idx:
                            _, c_path, c_cmd, c_runner = entry
                            break
                    with _lock:
                        _in_flight.pop(idx, None)
                        _wip = _worker_ips.get(info["worker"], "").split(":")[0]
                        _results.append({
                            "idx": idx, "worker": info["worker"],
                            "path": c_path, "cmd": c_cmd,
                            "rc": -1, "elapsed_ms": int(MAX_CASE_TIMEOUT * 1000),
                            "log_b64": "", "runner": c_runner,
                            "worker_ip": _wip, "slug": "",
                        })
                    print(f"[coordinator]   TIMEOUT case idx={idx}  "
                          f"worker={info['worker']}  {c_path}")
                last_progress = now

        with _lock:
            done     = len(_results)
            inflight = len(_in_flight)
            left     = (len(_queue_normal) - _pos_normal) + (len(_queue_largemem) - _pos_largemem)
            all_done = (_pos_normal  >= len(_queue_normal) and
                        _pos_largemem >= len(_queue_largemem) and
                        inflight == 0)
            workers_seen = list(_worker_last.keys())

        # 进度日志
        if done != done_prev:
            elapsed = int(time.time() - _start_time)
            pct = done * 100 // _total_cases if _total_cases else 0
            print(f"[coordinator] progress {done}/{_total_cases} ({pct}%)"
                  f"  in_flight={inflight}  queue={left}  elapsed={elapsed}s")
            done_prev = done
            last_progress = time.time()

        # in_flight 不为零时重置 last_progress，防止慢用例被误判为「无进度」
        if inflight > 0:
            last_progress = time.time()

        if all_done:
            break

        # 如果 large-mem 队列还有剩余但无 large-mem worker，发出警告
        with _lock:
            lm_left = len(_queue_largemem) - _pos_largemem
        if lm_left > 0:
            # 检查是否有已注册的 large-mem worker（通过 hostname 判断不了，
            # 简单地只在第一次发出提醒，避免刷屏）
            pass  # worker 能力由 run-test-dynamic 注册时携带，此处无需干预

        # 超时保护：长时间无活动
        if not workers_seen:
            if time.time() - _start_time > 120:
                print("[coordinator] TIMEOUT: no workers registered after 120s, exiting")
                break
        else:
            if time.time() - last_progress > MAX_WAIT:
                print(f"[coordinator] TIMEOUT: no progress for {MAX_WAIT}s, exiting")
                break

    server.shutdown()

    # 收割仍在 in_flight 的用例（协调器超时退出时 worker 还没有回报）
    with _lock:
        lost_cases = list(_in_flight.items())
    if lost_cases:
        print(f"[coordinator] WARN: {len(lost_cases)} in_flight case(s) never reported, marking as LOST")
        for idx, info in lost_cases:
            c_path, c_cmd, c_runner = "", "", "legacy"
            for entry in (_queue_normal + _queue_largemem):
                if entry[0] == idx:
                    _, c_path, c_cmd, c_runner = entry
                    break
            with _lock:
                _in_flight.pop(idx, None)
                _wip = _worker_ips.get(info["worker"], "").split(":")[0]
                _results.append({
                    "idx": idx, "worker": info["worker"],
                    "path": c_path, "cmd": c_cmd,
                    "rc": -2, "elapsed_ms": 0,
                    "log_b64": "", "runner": c_runner,
                    "worker_ip": _wip, "slug": "",
                })
            print(f"[coordinator]   LOST idx={idx}  worker={info['worker']}  {c_path}::{c_cmd}")

    # 汇总
    with _lock:
        results = list(_results)
    passes = sum(1 for r in results if r["rc"] == 0)
    fails  = len(results) - passes
    elapsed_total = int(time.time() - _start_time)

    print("\n" + "="*60)
    print(f"  AGGREGATE SUMMARY")
    print(f"  Total:   {len(results)} / {_total_cases}")
    print(f"  Pass:    {passes}")
    print(f"  Fail:    {fails}")
    print(f"  Elapsed: {elapsed_total}s")
    if _worker_scores:
        print(f"  Workers: {list(_worker_scores.keys())}")
    print("="*60)

    if fails:
        print("\nFailed cases:")
        for r in results:
            if r["rc"] != 0:
                _e = r.get('elapsed_ms', 0)
                _es = f"{_e//1000}.{_e%1000:03d}s"
                _wip = r.get('worker_ip', '')
                _slug = r.get('slug', '')  # worker 上报的完整路径: job-{id}/{nN-case_slug}
                if _wip and _slug:
                    _url = f"http://{_wip}:{_FAIL_HTTP_PORT}/{_slug}/"
                elif _wip:
                    _url = f"http://{_wip}:{_FAIL_HTTP_PORT}/"
                else:
                    _url = ""
                print(f"  [{r['worker']}] exit={r['rc']}  {_es}  {r['path']}::{r['cmd']}")
                if _url:
                    print(f"    {_url}")

        # 用 GitLab section 折叠块打印每个失败用例的日志
        import base64, time as _time
        print("")
        for r in results:
            if r["rc"] != 0:
                _e = r.get('elapsed_ms', 0)
                _es = f"{_e//1000:.0f}.{_e%1000:03d}s"
                label = f"{r['path']}::{r['cmd']}"
                sec_id = f"coord_fail_{r['idx']}"
                sec_title = f"FAIL [exit={r['rc']}] [{r['worker']}] {label}  ({_es})"
                _wip = r.get('worker_ip', '')
                _slug = r.get('slug', '')  # worker 上报的完整路径
                if _wip and _slug:
                    browse_url = f"http://{_wip}:{_FAIL_HTTP_PORT}/{_slug}/"
                elif _wip:
                    browse_url = f"http://{_wip}:{_FAIL_HTTP_PORT}/"
                else:
                    browse_url = ""
                ts = int(_time.time())
                _print_fail_sections(sec_id, sec_title, r.get("log_b64", ""),
                                     ts=ts, label=label, browse_url=browse_url)

    # 保存状态供 rerun 使用
    _save_state(results)

    # 写 JUnit
    junit_path = os.path.join(RESULTS_DIR, "junit-aggregate.xml")
    write_junit(results, junit_path)

    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
