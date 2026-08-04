import argparse
import html
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile
from pathlib import Path
from xml.etree import ElementTree


REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
BUILD_BINARIES = ("taosd.exe", "taos.exe", "taosBenchmark.exe", "taosadapter.exe", "taoskeeper.exe")
SERIAL_CTEST_PATTERN = "timerTest|cunit_test|pcre.*|example.*|clientTest|connectOptionsTest|tmqTest|taoscTest|cosTest|tcs_test|osFileTests|idxFstTest"
CTEST_READY_TIMEOUT_SECONDS = 300
GIT_LOCK_WAIT_SECONDS = 60


def run(command, *, cwd=None, env=None, stdout=None, stderr=None):
    print("+ " + " ".join(str(part) for part in command), flush=True)
    return subprocess.run(command, cwd=cwd, env=env, stdout=stdout, stderr=stderr, check=False)


def fail(message):
    raise RuntimeError(message)


def require_env(name):
    if os.environ.get(name):
        print(f"[windows-ci] {name} is set")
        return
    fail(
        f"required CI/CD variable {name} is not set. "
        "Configure it in GitLab Settings > CI/CD > Variables as a masked secret."
    )


def tail(path, lines=200):
    if not path.exists():
        return
    print(f"[windows-ci] tail of {path}:")
    print("\n".join(path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:]))


def stop_processes():
    for name in ("taosd", "taos", "taosBenchmark", "taosadapter", "taoskeeper"):
        subprocess.run(["taskkill", "/f", "/im", f"{name}.exe"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


def stop_build_processes():
    for name in ("cargo", "rustc", "git", "go", "jom", "cmake", "nmake", "cl", "link", "lib"):
        subprocess.run(["taskkill", "/f", "/t", "/im", f"{name}.exe"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


def remove_build_directory(debug_dir):
    if not debug_dir.exists():
        return
    print(f"[windows-ci] removing previous build directory: {debug_dir}")
    stop_processes()
    stop_build_processes()
    subprocess.run(["attrib", "-R", str(debug_dir / "*"), "/S", "/D"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    last_error = None
    for attempt in range(1, 6):
        try:
            shutil.rmtree(debug_dir)
            return
        except OSError as error:
            last_error = error
            print(f"[windows-ci] build cleanup attempt {attempt}/5 failed: {error}")
            time.sleep(2)
    fail(f"unable to remove previous build directory after retries: {last_error}")


def is_git_process_running():
    try:
        result = subprocess.run(
            ["tasklist", "/fi", "imagename eq git.exe", "/nh"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return False
    return "git.exe" in result.stdout.lower()


def remove_stale_git_index_lock(source_dir):
    lock = source_dir / ".git" / "index.lock"
    if not lock.exists():
        return
    print(f"[windows-ci] detected git index lock: {lock}")
    deadline = time.monotonic() + GIT_LOCK_WAIT_SECONDS
    while lock.exists() and time.monotonic() < deadline:
        if is_git_process_running():
            print("[windows-ci] waiting for active git.exe before removing index lock")
            time.sleep(2)
            continue
        try:
            lock.unlink()
            print("[windows-ci] removed stale git index lock")
            return
        except OSError as error:
            print(f"[windows-ci] unable to remove git index lock: {error}")
            time.sleep(2)
    if lock.exists():
        fail(f"git index lock remains after {GIT_LOCK_WAIT_SECONDS} seconds: {lock}")


def sync_source(source_dir):
    run(["git", "fetch", "origin", "--prune", "--force"], cwd=source_dir)
    remove_stale_git_index_lock(source_dir)
    for command in (("git", "reset", "--hard"), ("git", "clean", "-ffd")):
        if run(command, cwd=source_dir).returncode:
            fail(f"{' '.join(command)} failed")
    if os.environ.get("PARENT_PIPELINE_SOURCE") == "merge_request_event":
        merge_ref = f"merge-requests/{os.environ['PARENT_MR_IID']}/merge"
        if run(["git", "fetch", "origin", merge_ref], cwd=source_dir).returncode:
            fail("git fetch MR merge ref failed")
        checkout_ref = "FETCH_HEAD"
    else:
        checkout_ref = os.environ["CI_COMMIT_SHA"]
    for command in (("git", "checkout", "--detach", "-f", checkout_ref), ("git", "reset", "--hard", checkout_ref), ("git", "clean", "-ffd")):
        if run(command, cwd=source_dir).returncode:
            fail(f"{' '.join(command)} failed")

    print("[windows-ci] source sync complete; Windows CI does not update taos-ui submodules")


def visual_studio_environment():
    vcvars = Path(r"C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat")
    if not vcvars.exists():
        fail(f"Visual Studio environment script not found: {vcvars}")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".cmd", delete=False, encoding="utf-8", newline="\r\n") as command_file:
        command_file.write(f'@call "{vcvars}" x64\r\n')
        command_file.write("@if errorlevel 1 exit /b %errorlevel%\r\n")
        command_file.write("@set\r\n")
        command_path = command_file.name
    try:
        result = subprocess.run(
            ["cmd.exe", "/d", "/c", command_path],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
    finally:
        Path(command_path).unlink(missing_ok=True)
    if result.returncode:
        print(result.stdout)
        fail("vcvarsall.bat failed")
    environment = os.environ.copy()
    for line in result.stdout.splitlines():
        if "=" in line:
            name, value = line.split("=", 1)
            if name.upper() == "PATH":
                environment["PATH"] = value
                continue
            existing_name = next((key for key in environment if key.upper() == name.upper()), None)
            if existing_name is not None:
                del environment[existing_name]
            environment[name] = value
    return environment


def require_command(name, environment):
    if not shutil.which(name, path=environment["PATH"]):
        fail(f"{name} is not available after loading Visual Studio environment")


def install_go_toolchain(source_dir, environment):
    cache_root = Path(os.environ.get("WIN_GO_TOOLCHAIN_CACHE", source_dir.parent / "ci-cache" / "go-toolchains"))
    version = "v0.0.1-go1.26.4.windows-amd64"
    destination = cache_root / version
    go_binary = next(destination.glob("**/bin/go.exe"), None)
    if go_binary is None:
        cache_root.mkdir(parents=True, exist_ok=True)
        archive = cache_root / f"{version}.zip"
        shutil.rmtree(destination, ignore_errors=True)
        archive.unlink(missing_ok=True)
        print(f"[windows-ci] downloading Go toolchain {version}")
        urllib.request.urlretrieve(f"https://nexus.tdengine.net/repository/goproxy/golang.org/toolchain/@v/{version}.zip", archive)
        destination.mkdir(parents=True, exist_ok=True)
        if run(["tar.exe", "-xf", str(archive), "-C", str(destination)]).returncode:
            fail("Go toolchain extraction failed")
        go_binary = next(destination.glob("**/bin/go.exe"), None)
    if go_binary is None:
        fail(f"go.exe not found under {destination}")
    environment["GOROOT"] = str(go_binary.parent.parent)
    environment["PATH"] = f"{go_binary.parent};{environment['PATH']}"
    environment["GOTOOLCHAIN"] = "local"
    environment["GOPROXY"] = "https://nexus.tdengine.net/repository/goproxy/,direct"
    environment["GOSUMDB"] = "sum.golang.org"
    environment["GODEBUG"] = "http2client=0"


def build(source_dir, debug_dir):
    require_env("TD_ENTERPRISE_EDITION_SIGNATURE_SALT")
    sync_source(source_dir)
    remove_build_directory(debug_dir)
    if debug_dir.exists():
        fail(f"unable to remove previous build directory: {debug_dir}")
    debug_dir.mkdir(parents=True)
    environment = visual_studio_environment()
    for command in ("cl", "cmake", "jom", "go", "gcc"):
        require_command(command, environment)
    install_go_toolchain(source_dir, environment)

    configure = [
        "cmake", "..", "-G", "NMake Makefiles JOM", "-DCMAKE_MAKE_PROGRAM=jom", "-DCMAKE_BUILD_TYPE=Release",
        "-DBUILD_ENTERPRISE=ON", "-DBUILD_TEST=ON", "-DBUILD_TOOLS=ON", "-DBUILD_ADAPTER=ON", "-DBUILD_KEEPER=ON",
        "-DBUILD_RUST=ON", "-DBUILD_WITH_MARIADB=OFF", "-DBUILD_WITH_ARROW=OFF", "-DBUILD_ROCKSDB=ON",
        "-DBUILD_CONTRIB=ON", "-DBUILD_ASSERT_NOT_CORE=ON", "-DCPUTYPE=x64", "-DOSTYPE=Windows",
    ]
    environment["VSLANG"] = "1033"
    if run(configure, cwd=debug_dir, env=environment).returncode:
        fail("cmake failed")
    build_log = debug_dir / "build.log"
    with build_log.open("w", encoding="utf-8") as log_file:
        build_result = run(["jom", "-j8"], cwd=debug_dir, env=environment, stdout=log_file, stderr=subprocess.STDOUT)
    if build_result.returncode:
        tail(build_log, 300)
        run(["jom", "taosadapter"], cwd=debug_dir, env=environment)
        fail(f"jom build failed with exit code {build_result.returncode}")
    for binary in BUILD_BINARIES:
        if not (debug_dir / "build" / "bin" / binary).exists():
            fail(f"{binary} is missing from build output")
    taosws = debug_dir / "build" / "taos-connector-rust" / "target" / "release" / "taosws.dll"
    if not taosws.exists():
        run(["jom", "taos_connector_rust"], cwd=debug_dir, env=environment)
        fail("taosws.dll is missing from build output")


def write_ctest_failure_junit(path, name, message):
    suite = ElementTree.Element("testsuite", name=name, tests="1", failures="1", errors="0", skipped="0")
    case = ElementTree.SubElement(suite, "testcase", name=name, classname="windows.ctest")
    ElementTree.SubElement(case, "failure", message=message).text = message
    ElementTree.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)


def run_ctest_command(ctest_dir, debug_dir, group, *, include_pattern=None, exclude_pattern=None, jobs=1):
    junit = debug_dir / f"junit-windows-{group}.xml"
    log = debug_dir / f"ctest-{group}.log"
    command = ["ctest", "--test-dir", str(ctest_dir)]
    if include_pattern:
        command.extend(["-R", include_pattern])
    if exclude_pattern:
        command.extend(["-E", exclude_pattern])
    command.extend(["--output-on-failure", "--output-junit", str(junit), "--timeout", "900", f"-j{jobs}"])
    print(f"[windows-ci] running {group} ctest group (-j{jobs})")
    with log.open("w", encoding="utf-8") as log_file:
        result = run(command, stdout=log_file, stderr=subprocess.STDOUT)
    tail(log, 80)
    if not junit.exists():
        if result.returncode:
            write_ctest_failure_junit(junit, f"windows-{group}-ctest", f"ctest {group} group exited {result.returncode}; see {log.name}")
        else:
            ElementTree.ElementTree(ElementTree.Element("testsuite", name=f"windows-{group}-ctest", tests="0", failures="0", errors="0", skipped="0")).write(junit, encoding="utf-8", xml_declaration=True)
    return result.returncode


def merge_ctest_junit(debug_dir):
    destination = debug_dir / "junit-windows.xml"
    root = ElementTree.Element("testsuites", name="windows-unit")
    for group in ("parallel", "serial"):
        source = debug_dir / f"junit-windows-{group}.xml"
        if not source.exists():
            write_ctest_failure_junit(source, f"windows-{group}-ctest", f"ctest {group} group did not produce a report")
        parsed = ElementTree.parse(source).getroot()
        if parsed.tag == "testsuite":
            root.append(parsed)
        elif parsed.tag == "testsuites":
            root.extend(list(parsed))
    ElementTree.ElementTree(root).write(destination, encoding="utf-8", xml_declaration=True)


def write_ctest_failure_logs(debug_dir):
    failure_dir = debug_dir / "ctest-failures"
    shutil.rmtree(failure_dir, ignore_errors=True)
    failure_dir.mkdir()
    failures = []
    for group in ("parallel", "serial"):
        report = debug_dir / f"junit-windows-{group}.xml"
        if not report.exists():
            continue
        for case in ElementTree.parse(report).getroot().iter("testcase"):
            failure = case.find("failure")
            if failure is None:
                failure = case.find("error")
            if failure is None:
                continue
            test_name = case.get("name", "unknown-test")
            filename = f"{group}-{re.sub(r'[^A-Za-z0-9_.-]+', '_', test_name)}.log"
            details = [f"group: {group}", f"test: {test_name}"]
            if failure.get("message"):
                details.append(f"failure: {failure.get('message')}")
            if failure.text:
                details.extend(("", failure.text.strip()))
            output = case.findtext("system-out")
            if output:
                details.extend(("", output.strip()))
            (failure_dir / filename).write_text("\n".join(details) + "\n", encoding="utf-8")
            failures.append((group, test_name, filename))
    return failures


def print_ctest_failure_logs(debug_dir, failures):
    if not failures:
        return
    failure_dir = debug_dir / "ctest-failures"
    print("\x1b[31m" + "=" * 80 + "\x1b[0m")
    print("\x1b[31m[windows-ci] CTEST FAILURE SUMMARY\x1b[0m")
    for group, test_name, filename in failures:
        section_name = f"ctest_failure_{re.sub(r'[^A-Za-z0-9_]+', '_', filename)}"
        print(f"\x1b[31m[windows-ci] FAILED [{group}] {test_name}\x1b[0m")
        print(f"\x1b[0Ksection_start:{int(time.time())}:{section_name}[collapsed=true]\r\x1b[0K\x1b[31m▶ full {group} failure log: {test_name}\x1b[0m")
        print(f"\x1b[31m{(failure_dir / filename).read_text(encoding='utf-8', errors='replace')}\x1b[0m", end="")
        print(f"\x1b[0Ksection_end:{int(time.time())}:{section_name}\r\x1b[0K")
    for group in sorted({group for group, _, _ in failures}):
        group_log = debug_dir / f"ctest-{group}.log"
        if not group_log.exists():
            continue
        section_name = f"ctest_{group}_full_log"
        print(f"\x1b[0Ksection_start:{int(time.time())}:{section_name}[collapsed=true]\r\x1b[0K\x1b[31m▶ full {group} ctest log\x1b[0m")
        print(f"\x1b[31m{group_log.read_text(encoding='utf-8', errors='replace')}\x1b[0m", end="")
        print(f"\x1b[0Ksection_end:{int(time.time())}:{section_name}\r\x1b[0K")
    print("\x1b[31m" + "=" * 80 + "\x1b[0m")


def run_ctest(source_dir, debug_dir, ctest_dir):
    taosd, taos = (debug_dir / "build" / "bin" / name for name in ("taosd.exe", "taos.exe"))
    stdout_log, stderr_log, health_log = (debug_dir / name for name in ("taosd-ctest.stdout.log", "taosd-ctest.stderr.log", "taosd-ctest-health.log"))
    parallel_result = 1
    with stdout_log.open("w", encoding="utf-8") as stdout_file, stderr_log.open("w", encoding="utf-8") as stderr_file:
        server = subprocess.Popen([str(taosd)], cwd=source_dir, stdout=stdout_file, stderr=stderr_file)
        try:
            with health_log.open("w", encoding="utf-8") as health_file:
                ready = False
                deadline = time.monotonic() + CTEST_READY_TIMEOUT_SECONDS
                while time.monotonic() < deadline and server.poll() is None:
                    if subprocess.run([str(taos), "-s", "select 1"], stdout=health_file, stderr=subprocess.STDOUT, check=False).returncode == 0:
                        ready = True
                        break
                    time.sleep(2)
            if not ready:
                message = f"taosd did not become query-ready within {CTEST_READY_TIMEOUT_SECONDS} seconds"
                print(f"[windows-ci] {message}")
                write_ctest_failure_junit(debug_dir / "junit-windows-parallel.xml", "windows-parallel-ctest", message)
            else:
                parallel_result = run_ctest_command(ctest_dir, debug_dir, "parallel", exclude_pattern=SERIAL_CTEST_PATTERN, jobs=4)
        finally:
            stop_processes()
    serial_result = run_ctest_command(ctest_dir, debug_dir, "serial", include_pattern=SERIAL_CTEST_PATTERN, jobs=1)
    merge_ctest_junit(debug_dir)
    failures = write_ctest_failure_logs(debug_dir)
    print(f"[windows-ci] ctest result: parallel={parallel_result}, serial={serial_result}")
    return (1 if parallel_result or serial_result else 0), failures


def run_pytest(source_dir, debug_dir, log_dir):
    bin_dir = debug_dir / "build" / "bin"
    system_dir = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32"
    for name in ("taos.dll", "pthreadVC3.dll", "taosnative.dll"):
        if (bin_dir / name).exists():
            shutil.copy2(bin_dir / name, system_dir / name)
    taosws = debug_dir / "build" / "taos-connector-rust" / "target" / "release" / "taosws.dll"
    if taosws.exists():
        shutil.copy2(taosws, system_dir / taosws.name)
    stop_processes()
    time.sleep(3)
    log_dir.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment["PATH"] = f"{bin_dir}{os.pathsep}{environment['PATH']}"
    test_dir = source_dir / "source" / "taos-community" / "test"
    return run([sys.executable, "ci/run_win_cases.py", "ci/win_cases.task", str(log_dir)], cwd=test_dir, env=environment).returncode


def parse_result_records(result_file):
    if result_file is None:
        return []
    results = []
    for line in result_file.read_text(encoding="utf-8", errors="replace").splitlines():
        fields = line.split("\t")
        if len(fields) < 2 or fields[0] not in {"SUCCESS", "FAILED", "ERROR", "Skip"}:
            continue
        duration = next((field[:-1] for field in fields if field.endswith("s") and field[:-1].replace(".", "", 1).isdigit()), "0")
        results.append((fields[0], fields[1], duration, "\t".join(fields[5:]).strip()))
    return results


def parse_results(result_file):
    return [(status, command, duration) for status, command, duration, _ in parse_result_records(result_file)]


def print_pytest_failure_logs(log_dir):
    result_files = sorted(log_dir.glob("result_*.txt"))
    records = parse_result_records(result_files[-1] if result_files else None)
    failures = [record for record in records if record[0] in {"FAILED", "ERROR"}]
    archives = sorted(log_dir.glob("log_*.zip"))
    if not failures:
        return []
    reports = []
    archive = zipfile.ZipFile(archives[-1]) if archives else None
    try:
        members = {Path(member).name: member for member in archive.namelist()} if archive else {}
        for status, command, _, detail in failures:
            log_name = command.split("/")[-1].replace(" ", "_") + ".log"
            output = archive.read(members[log_name]).decode("utf-8", errors="replace") if archive and log_name in members else (detail or "pytest runner did not produce a case log")
            reports.append((status, command, log_name, output))
    finally:
        if archive:
            archive.close()
    return reports


def print_pytest_failure_reports(reports):
    if not reports:
        return
    print("\x1b[31m" + "=" * 80 + "\x1b[0m")
    print("\x1b[31m[windows-ci] PYTEST FAILURE SUMMARY\x1b[0m")
    for status, command, log_name, output in reports:
        section_name = f"pytest_failure_{re.sub(r'[^A-Za-z0-9_]+', '_', log_name)}"
        print(f"\x1b[31m[windows-ci] {status} {command}\x1b[0m")
        print(f"\x1b[0Ksection_start:{int(time.time())}:{section_name}[collapsed=true]\r\x1b[0K\x1b[31m▶ full pytest failure log: {log_name}\x1b[0m")
        print(f"\x1b[31m{output}\x1b[0m", end="" if output.endswith("\n") else "\n")
        print(f"\x1b[0Ksection_end:{int(time.time())}:{section_name}\r\x1b[0K")
    print("\x1b[31m" + "=" * 80 + "\x1b[0m")


def collect_artifacts(debug_dir, log_dir, artifact_dir):
    artifact_dir.mkdir(parents=True, exist_ok=True)
    for name in ("build.log", "CMakeFiles/CMakeOutput.log", "CMakeFiles/CMakeError.log", "taosd-ctest.stdout.log", "taosd-ctest.stderr.log", "taosd-ctest-health.log", "ctest-parallel.log", "ctest-serial.log", "junit-windows-parallel.xml", "junit-windows-serial.xml"):
        source = debug_dir / name
        if source.exists():
            shutil.copy2(source, artifact_dir / source.name)
    junit = debug_dir / "junit-windows.xml"
    if junit.exists():
        shutil.copy2(junit, artifact_dir / junit.name)
    else:
        (artifact_dir / junit.name).write_text('<?xml version="1.0" encoding="UTF-8"?><testsuite name="windows-unit" tests="0" failures="0" errors="0" skipped="0" />', encoding="utf-8")
    ctest_artifacts = artifact_dir / "ctest"
    ctest_artifacts.mkdir(exist_ok=True)
    rows = []
    for group in ("parallel", "serial"):
        log = debug_dir / f"ctest-{group}.log"
        report = debug_dir / f"junit-windows-{group}.xml"
        rows.append(f'<tr><td>{group}</td><td><a href="../{html.escape(log.name)}">ctest log</a></td><td><a href="../{html.escape(report.name)}">JUnit XML</a></td></tr>')
    source_failure_dir = debug_dir / "ctest-failures"
    artifact_failure_dir = ctest_artifacts / "failures"
    if source_failure_dir.exists():
        shutil.copytree(source_failure_dir, artifact_failure_dir, dirs_exist_ok=True)
    failure_rows = "".join(f'<li><a href="failures/{html.escape(path.name)}">{html.escape(path.stem)}</a></li>' for path in sorted(artifact_failure_dir.glob("*.log")))
    failure_section = f"<h2>Failed tests</h2><ul>{failure_rows}</ul>" if failure_rows else "<h2>Failed tests</h2><p>None.</p>"
    ctest_report = '<!doctype html><html><head><meta charset="utf-8"><title>Windows unit-test report</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:2rem;color:#1f2937}table{border-collapse:collapse}th,td{border:1px solid #d1d5db;padding:.55rem;text-align:left}th{background:#f3f4f6}</style></head><body><h1>Windows unit-test report</h1><p>Parallel tests share one ready taosd; serial tests manage their own taosd and run with -j1.</p><table><thead><tr><th>Group</th><th>Console log</th><th>GitLab test report</th></tr></thead><tbody>' + ''.join(rows) + '</tbody></table>' + failure_section + '</body></html>'
    (ctest_artifacts / "index.html").write_text(ctest_report, encoding="utf-8")
    pytest_artifacts = artifact_dir / "pytest"
    pytest_artifacts.mkdir(exist_ok=True)
    result_files = sorted(log_dir.glob("result_*.txt"))
    result_file = result_files[-1] if result_files else None
    if result_file:
        shutil.copy2(result_file, pytest_artifacts / "result.txt")
    archives = sorted(log_dir.glob("log_*.zip"))
    for archive in archives:
        copied = pytest_artifacts / archive.name
        shutil.copy2(archive, copied)
        with zipfile.ZipFile(copied) as zip_file:
            zip_file.extractall(pytest_artifacts / archive.stem)
    results = parse_results(result_file)
    suite = ElementTree.Element("testsuite", name="windows-selected-pytest", tests=str(len(results)), failures=str(sum(status == "FAILED" for status, _, _ in results)), errors=str(sum(status == "ERROR" for status, _, _ in results)), skipped=str(sum(status == "Skip" for status, _, _ in results)))
    for status, command, duration in results:
        case = ElementTree.SubElement(suite, "testcase", name=command, classname="windows.pytest", time=duration)
        if status in {"FAILED", "ERROR"}:
            ElementTree.SubElement(case, status.lower(), message="pytest command failed; see pytest artifact logs").text = command
        elif status == "Skip":
            ElementTree.SubElement(case, "skipped").text = "listed in win_ignore_cases"
    ElementTree.ElementTree(suite).write(pytest_artifacts / "junit-pytest-windows.xml", encoding="utf-8", xml_declaration=True)
    archive = archives[0] if len(archives) == 1 else None
    rows = []
    for status, command, duration in results:
        link = ""
        if archive and status in {"FAILED", "ERROR"}:
            link = f'<a href="{html.escape(archive.stem + "/" + command.rsplit("/", 1)[-1].replace(" ", "_") + ".log")}">open log</a>'
        rows.append(f'<tr><td class="{html.escape(status)}">{html.escape(status)}</td><td><code>{html.escape(command)}</code></td><td>{html.escape(duration)} s</td><td>{link}</td></tr>')
    report = f'<!doctype html><html><head><meta charset="utf-8"><title>Windows pytest report</title><style>body{{font-family:Segoe UI,Arial,sans-serif;margin:2rem;color:#1f2937}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #d1d5db;padding:.55rem;text-align:left}}th{{background:#f3f4f6}}.SUCCESS{{color:#15803d}}.FAILED,.ERROR{{color:#b91c1c;font-weight:600}}.Skip{{color:#6b7280}}</style></head><body><h1>Windows selected pytest report</h1><table><thead><tr><th>Status</th><th>Command</th><th>Duration</th><th>Failure log</th></tr></thead><tbody>{"".join(rows)}</tbody></table></body></html>'
    (pytest_artifacts / "index.html").write_text(report, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, default=Path(os.environ.get("WIN_TSDB_SRC", REPOSITORY_ROOT)))
    parser.add_argument("--debug-dir", type=Path, default=Path(os.environ.get("WIN_DEBUG_DIR", REPOSITORY_ROOT / "debug")))
    parser.add_argument("--ctest-dir", type=Path, default=Path(os.environ.get("WIN_CTEST_DIR", REPOSITORY_ROOT / "debug" / "source" / "taos-community")))
    parser.add_argument("--log-dir", type=Path, default=Path(os.environ.get("WIN_LOG_DIR", REPOSITORY_ROOT / "ci-log")) / f"PR-{os.environ.get('PARENT_MR_IID', 'local')}-{os.environ.get('CI_PIPELINE_ID', 'local')}")
    parser.add_argument("--artifact-dir", type=Path, default=Path(os.environ.get("CI_PROJECT_DIR", REPOSITORY_ROOT)) / "ci-artifacts" / "windows")
    parser.add_argument("--skip-build", action="store_true", default=os.environ.get("WIN_SKIP_BUILD", "").lower() in {"1", "true", "yes"})
    parser.add_argument("--skip-pytest", action="store_true", default=os.environ.get("WIN_SKIP_PYTEST", "").lower() in {"1", "true", "yes"})
    arguments = parser.parse_args()
    result = 1
    ctest_result = 1
    ctest_failures = []
    pytest_result = 1
    pytest_failures = []
    try:
        if arguments.skip_build:
            print("[windows-ci] build is skipped; reusing existing Windows build output")
        else:
            build(arguments.source_dir, arguments.debug_dir)
        ctest_result, ctest_failures = run_ctest(arguments.source_dir, arguments.debug_dir, arguments.ctest_dir)
        if arguments.skip_pytest:
            print("[windows-ci] pytest is skipped for this ctest-only validation run")
            pytest_result = 0
        else:
            pytest_result = run_pytest(arguments.source_dir, arguments.debug_dir, arguments.log_dir)
            pytest_failures = print_pytest_failure_logs(arguments.log_dir)
        result = 1 if ctest_result or pytest_result else 0
        print(f"[windows-ci] result: ctest={ctest_result}, pytest={pytest_result}")
        print_ctest_failure_logs(arguments.debug_dir, ctest_failures)
        print_pytest_failure_reports(pytest_failures)
    except Exception as error:
        print(f"[windows-ci] failed: {error}")
    finally:
        stop_processes()
        stop_build_processes()
        collect_artifacts(arguments.debug_dir, arguments.log_dir, arguments.artifact_dir)
    return result


if __name__ == "__main__":
    sys.exit(main())
