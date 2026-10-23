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

import html
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from xml.etree import ElementTree


# Match Linux unit-test: sessionTest is compiled but not executed in CI.
# Linux puts it in CTEST_EXCLUDE and does not re-run it after parallel ctest.
SERIAL_TESTS = "clientTest|connectOptionsTest|timerTest|streamTriggerTaskTest"
PARALLEL_EXCLUDES = f"{SERIAL_TESTS}|sessionTest|pcre.*|example.*"
TAOSD_STOP_TIMEOUT = 10


def taosd_pids():
    result = subprocess.run(
        ["ps", "-axo", "pid=,stat=,comm="],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"ps failed while checking taosd: {result.stderr.strip()}")
    pids = []
    for line in result.stdout.splitlines():
        fields = line.split(maxsplit=2)
        if len(fields) == 3 and not fields[1].startswith("Z") and Path(fields[2]).name == "taosd":
            pids.append(int(fields[0]))
    return sorted(pids)


def cleanup_taosd():
    pids = taosd_pids()
    if not pids:
        return
    print(f"[macos-ci] removing stale taosd: pid={pids}", flush=True)
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    deadline = time.monotonic() + TAOSD_STOP_TIMEOUT
    remaining = taosd_pids()
    while remaining and time.monotonic() < deadline:
        time.sleep(0.2)
        remaining = taosd_pids()

    for pid in remaining:
        print(f"[macos-ci] force-stopping taosd pid={pid} with SIGKILL", flush=True)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    if remaining:
        time.sleep(0.2)
    remaining = taosd_pids()
    if remaining:
        raise RuntimeError(f"failed to stop taosd: pid={remaining}")


def stop_taosd_process(server, log):
    try:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
    finally:
        log.close()


def parse_group_results(log):
    content = log.read_text(encoding="utf-8", errors="replace")
    results = {}
    for test_id, name, status in re.findall(r"^\s*(\d+)\s+-\s+(\S+) \(([^)]+)\)$", content, re.MULTILINE):
        results[int(test_id)] = {"name": name, "status": status, "detail": ""}
    for match in re.finditer(
        r"^\s*(\d+)/(\d+) Test\s+#(\d+):\s+(\S+).*?(Passed|\*+(?:Failed|Exception|Timeout|Not Run|Skipped)).*?(\d+\.\d+) sec",
        content,
        re.MULTILINE,
    ):
        _, _, test_id, name, status, duration = match.groups()
        result = results.setdefault(int(test_id), {"name": name, "status": status, "detail": ""})
        result["name"] = name
        result["duration"] = duration
        if status != "Passed":
            existing_status = result["status"]
            result["status"] = status.lstrip("*") if existing_status == status else f"{status.lstrip('*')} ({existing_status})"
    for result in results.values():
        if result["status"] != "Passed":
            result["detail"] = "See the full CTest group log; parallel test output may be interleaved."
    return list(results.values())


def run_group(debug_dir, artifact_dir, name, environment, *, include=None, exclude=None, jobs=1):
    log = artifact_dir / f"ctest-{name}.log"
    command = ["ctest", "--test-dir", str(debug_dir), "--timeout", "900", "--output-on-failure"]
    if include:
        command.extend(["-R", include])
    if exclude:
        command.extend(["-E", exclude])
    command.append(f"-j{jobs}")
    print(f"[macos-ci] running {name} ctest group (-j{jobs})", flush=True)
    with log.open("w", encoding="utf-8") as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=False, env=environment)
    if result.returncode:
        print(log.read_text(encoding="utf-8", errors="replace")[-8000:], end="", flush=True)
    results = parse_group_results(log)
    if not results:
        print(f"[macos-ci] ERROR: no CTest results parsed for {name}", flush=True)
        results = [{"name": f"{name}-ctest-parser", "status": "ParseError", "detail": "CTest output did not match the expected result format"}]
        return 1, results
    # Print the per-test outcome as well: ctest only writes a passing test's output to
    # the log file, so without this the job log shows just the group banner and the
    # final count, and a case that did not run (or skipped) is invisible there.
    for case in results:
        duration = f"  {case['duration']} sec" if case.get("duration") else ""
        print(f"[macos-ci] {case['name']}: {case['status']}{duration}", flush=True)
    return result.returncode, results


def start_taosd(debug_dir, artifact_dir):
    bin_dir = debug_dir / "build" / "bin"
    taosd = bin_dir / "taosd"
    taos = bin_dir / "taos"
    config_dir = artifact_dir / "taos-config"
    data_dir = config_dir / "data"
    log_dir = config_dir / "log"
    config_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(exist_ok=True)
    log_dir.mkdir(exist_ok=True)
    (config_dir / "taos.cfg").write_text(
        "\n".join(
            (
                "firstEp 127.0.0.1:6030",
                "fqdn 127.0.0.1",
                "serverPort 6030",
                f"dataDir {data_dir}",
                f"logDir {log_dir}",
                "asyncLog 0",
                "monitor 0",
                "supportVnodes 1024",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    log = (artifact_dir / "taosd.log").open("w", encoding="utf-8")
    environment = dict(os.environ)
    environment["PATH"] = f"{bin_dir}:{environment.get('PATH', '')}"
    environment["DYLD_LIBRARY_PATH"] = f"{debug_dir / 'build' / 'lib'}:{environment.get('DYLD_LIBRARY_PATH', '')}"
    environment["TAOS_TEST_CONFIG_DIR"] = str(config_dir)
    try:
        server = subprocess.Popen([str(taosd), "-c", str(config_dir)], cwd=debug_dir.parent, env=environment, stdout=log, stderr=subprocess.STDOUT)
    except OSError:
        log.close()
        raise
    deadline = time.monotonic() + 300
    ready = False
    while time.monotonic() < deadline and server.poll() is None:
        try:
            probe = subprocess.run(
                [str(taos), "-c", str(config_dir), "-s", "select 1"],
                cwd=debug_dir.parent,
                env=environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=10,
            )
        except subprocess.TimeoutExpired:
            time.sleep(2)
            continue
        except OSError:
            stop_taosd_process(server, log)
            raise
        if probe.returncode == 0:
            ready = True
            break
        time.sleep(2)
    if not ready:
        log.flush()
        stop_taosd_process(server, log)
        details = Path(log.name).read_text(encoding="utf-8", errors="replace")[-4000:]
        print(f"[macos-ci] taosd startup log:\n{details}", flush=True)
        raise RuntimeError("taosd did not become query-ready within 300 seconds")
    return server, log, environment


def write_junit(artifact_dir, group_results):
    destination = artifact_dir / "junit-macos.xml"
    root = ElementTree.Element("testsuites", name="macos-unit")
    for group, results in group_results.items():
        failures = sum(result["status"] != "Passed" for result in results)
        suite = ElementTree.SubElement(root, "testsuite", name=f"macos.{group}", tests=str(len(results)), failures=str(failures))
        for result in results:
            case = ElementTree.SubElement(suite, "testcase", name=result["name"], classname=f"macos.{group}", time=result.get("duration", "0"))
            if result["status"] != "Passed":
                failure = ElementTree.SubElement(case, "failure", message=result["status"])
                failure.text = result["detail"] or f"CTest status: {result['status']}"
        group_root = ElementTree.Element("testsuites", name=f"macos-{group}")
        group_root.append(suite)
        ElementTree.ElementTree(group_root).write(artifact_dir / f"junit-macos-{group}.xml", encoding="utf-8", xml_declaration=True)
    ElementTree.ElementTree(root).write(destination, encoding="utf-8", xml_declaration=True)
    return destination


def failed_cases(group_results):
    return [(group, result["name"], result["detail"] or result["status"])
            for group, results in group_results.items() for result in results if result["status"] != "Passed"]


def print_summary(group_results):
    results = [result for group in group_results.values() for result in group]
    passed = sum(result["status"] == "Passed" for result in results)
    failures = len(results) - passed
    print(f"[macos-ci] SUMMARY: total={len(results)} passed={passed} failed={failures}", flush=True)
    for group, group_items in group_results.items():
        group_failures = [item for item in group_items if item["status"] != "Passed"]
        print(f"[macos-ci] {group}: total={len(group_items)} passed={len(group_items) - len(group_failures)} failed={len(group_failures)}", flush=True)
        for result in group_failures:
            print(f"[macos-ci] FAIL {group}/{result['name']}: {result['status']}", flush=True)


def print_failure_sections(artifact_dir, failures, group_results):
    for group, test, detail in failures:
        section = f"macos_failure_{re.sub(r'[^A-Za-z0-9_]+', '_', group + '_' + test)}"
        print(f"\033[31m[macos-ci] FAILED [{group}] {test}\033[0m", flush=True)
        print(f"\033[0Ksection_start:{int(time.time())}:{section}[collapsed=true]\r\033[0K▶ {test} failure detail", flush=True)
        print(detail.strip(), flush=True)
        print(f"\033[0Ksection_end:{int(time.time())}:{section}\r\033[0K", flush=True)
    failed_groups = {group for group, results in group_results.items() if any(result["status"] != "Passed" for result in results)}
    for group in failed_groups:
        log = artifact_dir / f"ctest-{group}.log"
        if not log.exists():
            continue
        section = f"macos_ctest_{group}_full_log"
        print(f"\033[0Ksection_start:{int(time.time())}:{section}[collapsed=true]\r\033[0K▶ full {group} ctest log", flush=True)
        print(log.read_text(encoding="utf-8", errors="replace")[-8000:], end="", flush=True)
        print(f"\033[0Ksection_end:{int(time.time())}:{section}\r\033[0K", flush=True)


def write_html(artifact_dir, group_results, failures):
    rows = []
    for group in ("parallel", "serial"):
        log = f"ctest-{group}.log"
        junit = f"junit-macos-{group}.xml"
        rows.append(f"<tr><td>{group}</td><td><a href=\"{html.escape(log)}\">log</a></td><td><a href=\"{html.escape(junit)}\">JUnit</a></td></tr>")
    results = [result for group in group_results.values() for result in group]
    passed = sum(result["status"] == "Passed" for result in results)
    failure_rows = "".join(f"<li>{html.escape(group)}: {html.escape(test)} — {html.escape(detail)}</li>" for group, test, detail in failures)
    report = "<!doctype html><html><head><meta charset='utf-8'><title>macOS CTest report</title></head><body>"
    report += "<h1>macOS CTest report</h1><p>Parallel and serial groups run with a shared taosd.</p>"
    report += f"<h2>Summary</h2><p>Total: {len(results)} | Passed: {passed} | Failed: {len(results) - passed}</p>"
    report += "<table border='1'><tr><th>Group</th><th>Log</th><th>JUnit</th></tr>" + "".join(rows) + "</table>"
    report += "<h2>Failed tests</h2><ul>" + (failure_rows or "<li>None</li>") + "</ul></body></html>"
    (artifact_dir / "index.html").write_text(report, encoding="utf-8")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--cleanup":
        cleanup_taosd()
        return 0

    debug_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "debug").resolve()
    artifact_dir = Path(sys.argv[2] if len(sys.argv) > 2 else "macos-ci-artifacts").resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    cleanup_taosd()
    server, taosd_log, environment = start_taosd(debug_dir, artifact_dir)
    try:
        parallel_rc, parallel_results = run_group(debug_dir, artifact_dir, "parallel", environment, exclude=PARALLEL_EXCLUDES, jobs=10)
        serial_rc, serial_results = run_group(debug_dir, artifact_dir, "serial", environment, include=f"^({SERIAL_TESTS})$", jobs=1)
    finally:
        stop_taosd_process(server, taosd_log)
    group_results = {"parallel": parallel_results, "serial": serial_results}
    write_junit(artifact_dir, group_results)
    failures = failed_cases(group_results)
    print_summary(group_results)
    print_failure_sections(artifact_dir, failures, group_results)
    write_html(artifact_dir, group_results, failures)
    print(f"[macos-ci] ctest result: parallel={parallel_rc}, serial={serial_rc}", flush=True)
    return 1 if parallel_rc or serial_rc else 0


if __name__ == "__main__":
    raise SystemExit(main())
