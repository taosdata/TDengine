#!/usr/bin/env bash
# =============================================================================
# aggregate-report.sh — 汇总所有节点的 JUnit XML，输出 PASS/FAIL 统计摘要
# =============================================================================
# 此 job 在 collect-artifacts 阶段运行（`when: always`），汇总 test-linux 的结果。
# GitLab 在 `needs:[{job:test-linux, artifacts:true}]` 时会自动合并所有并行实例
# 的 artifacts 到同一路径，因此 results/junit-*.xml 包含全部节点的结果。
#
# 调用方式：
#   bash tests/ci/scripts/aggregate-report.sh
# =============================================================================
set -uo pipefail

RESULTS_DIR="${CI_PROJECT_DIR:-${PWD}}/results"
# 只收集各 worker 的 junit-N.xml，排除 coordinator 生成的 junit-aggregate.xml（避免重复计数）
JUNIT_GLOB="${RESULTS_DIR}/junit-[0-9]*.xml"
PIPELINE_URL="${CI_PIPELINE_URL:-}"
PROJECT_URL="${CI_PROJECT_URL:-}"

echo "========================================"
echo " Aggregate Test Report"
echo " Pipeline: ${PIPELINE_URL}"
echo "========================================"

shopt -s nullglob
JUNIT_FILES=( ${JUNIT_GLOB} )

if [[ ${#JUNIT_FILES[@]} -eq 0 ]]; then
    echo "WARNING: No JUnit XML files found at ${JUNIT_GLOB}"
    exit 0
fi

echo "Found ${#JUNIT_FILES[@]} JUnit file(s): ${JUNIT_FILES[*]}"
echo ""

# --------------------------------------------------
# 用 python3 解析 XML，输出 pass/fail 明细
# --------------------------------------------------
python3 - "${RESULTS_DIR}" "${PROJECT_URL}" "${PIPELINE_URL}" "${CI_COMMIT_SHA:-HEAD}" << 'PYEOF'
import sys
import os
import glob
import xml.etree.ElementTree as ET

results_dir = sys.argv[1] if len(sys.argv) > 1 else "results"
project_url = sys.argv[2] if len(sys.argv) > 2 else ""
pipeline_url = sys.argv[3] if len(sys.argv) > 3 else ""
commit_sha   = sys.argv[4] if len(sys.argv) > 4 else "HEAD"

total_tests = 0
total_pass  = 0
total_fail  = 0
total_error = 0
failures = []    # list of (suite_name, case_name, classname, message, log_hint)

xml_files = sorted(f for f in glob.glob(os.path.join(results_dir, "junit-*.xml"))
               if not os.path.basename(f).startswith("junit-aggregate"))
for xml_file in xml_files:
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        suite = root if root.tag == 'testsuite' else root.find('testsuite')
        if suite is None:
            continue
        suite_name = suite.get('name', os.path.basename(xml_file))
        tests    = int(suite.get('tests', 0))
        failures_n = int(suite.get('failures', 0))
        errors_n   = int(suite.get('errors', 0))
        total_tests += tests
        total_fail  += failures_n
        total_error += errors_n
        total_pass  += tests - failures_n - errors_n

        for tc in suite.findall('testcase'):
            failure = tc.find('failure')
            error   = tc.find('error')
            if failure is not None or error is not None:
                elem = failure if failure is not None else error
                msg  = (elem.get('message', '') or '').strip()
                text = (elem.text or '').strip()
                # 提取 log artifact 路径（run-test-batch.sh 注入的首行）
                log_hint = ""
                log_lines = []
                for line in text.splitlines():
                    if line.startswith("Log artifact:"):
                        log_hint = line.replace("Log artifact:", "").strip()
                    else:
                        log_lines.append(line)
                log_text = "\n".join(log_lines).strip()
                failures.append((
                    suite_name,
                    tc.get('name', '?'),
                    tc.get('classname', '?'),
                    msg,
                    log_hint,
                    log_text,
                ))
    except Exception as e:
        print(f"  WARNING: failed to parse {xml_file}: {e}")

print("=" * 64)
print(f"  TOTAL:   {total_tests}")
print(f"  PASS:    {total_pass}")
print(f"  FAIL:    {total_fail}")
print(f"  ERROR:   {total_error}")
print("=" * 64)

if failures:
    print(f"\nFailed {len(failures)} case(s):\n")
    for i, (suite, name, classname, msg, log_hint, log_text) in enumerate(failures, 1):
        print(f"  [{i:3d}] {name}")
        print(f"         Suite:    {suite}")
        print(f"         Class:    {classname}")
        if msg:
            print(f"         Message:  {msg}")
        if log_hint:
            if project_url:
                # suite_name like 'test-linux-node3-dynamic' → job='test-linux-3'
                import re as _re
                m = _re.search(r'node(\d+)', suite)
                job_name = f"test-linux-{m.group(1)}" if m else "test-linux"
                artifact_url = f"{project_url}/-/jobs/artifacts/{commit_sha}/raw/{log_hint}?job={job_name}"
                print(f"         Log:      {artifact_url}")
            else:
                print(f"         Log(rel): {log_hint}")
        print()

    print("=" * 64)
    print("Detailed failure logs (collapsed, click to expand):")
    print("=" * 64)
    import time as _time
    for i, (suite, name, classname, msg, log_hint, log_text) in enumerate(failures, 1):
        ts = int(_time.time() * 1000) + i
        sec_id = f"agg_fail_{i}_{ts}"
        sec_title = f"FAIL [{i}/{len(failures)}] {name}  ({suite})"
        # section_start（collapsed）
        print(f"\x1b[0Ksection_start:{ts}:{sec_id}[collapsed=true]\r\x1b[0K\x1b[31;1m{sec_title}\x1b[0m")
        print("─" * 64)
        if log_text:
            print(log_text[:4096])
        else:
            print("(no log captured in JUnit XML)")
        print("─" * 64)
        print(f"\x1b[0Ksection_end:{ts}:{sec_id}\r\x1b[0K")
else:
    print("\nAll tests passed! ✓")

sys.exit(1 if total_fail + total_error > 0 else 0)
PYEOF

exit $?
