#!/bin/bash
# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Behavioural regression suite for packaging/docker/bin/entrypoint.sh.
#
# The script only supervises processes, so static checks cannot catch its
# regressions: every scenario here starts a container with stubbed binaries and
# observes what docker reports. Requires a local docker daemon.
#
# Usage: bash packaging/docker/test/run-local.sh [-k]   (-k keeps containers)
set -u
cd "$(dirname "$0")" || exit 1
DOCKER_DIR=$(cd .. && pwd)
KEEP=0; [ "${1:-}" = "-k" ] && KEEP=1

pass=0; fail=0
ok()  { echo "  PASS  $1"; pass=$((pass + 1)); }
bad() { echo "  FAIL  $1"; fail=$((fail + 1)); }
state() { docker inspect -f '{{.State.Status}}/{{.State.ExitCode}}' "$1" 2>/dev/null; }
cleanup() { [ "$KEEP" = "1" ] || docker rm -f ep-normal-1 ep-normal-2 ep-fail-1 \
                                    ep-slow-1 ep-noep-1 >/dev/null 2>&1; }
trap cleanup EXIT

build() {  # build <tag> <stub dir relative to packaging/docker>
    ( cd "$DOCKER_DIR" && docker build -q -t "$1" -f test/Dockerfile.test \
        --build-arg STUBS="$2" . ) >/dev/null 2>&1 \
        || { echo "build $1 failed"; exit 1; }
}
wait_state() {  # wait_state <container> <expected> <max seconds>
    for _ in $(seq 1 $(( $3 * 2 ))); do
        [ "$(state "$1")" = "$2" ] && return 0
        sleep 0.5
    done
    return 1
}

echo "building images..."
build ep-normal test/stubs
build ep-fail   test/stubs-fail
build ep-slow   test/stubs-slow
echo

echo "[1] normal start"
docker rm -f ep-normal-1 >/dev/null 2>&1
docker run -d --name ep-normal-1 ep-normal >/dev/null
sleep 4
[ "$(state ep-normal-1)" = "running/0" ] && ok "container running" || bad "state=$(state ep-normal-1)"
n=$(docker exec ep-normal-1 sh -c 'ls /tmp/*.pid 2>/dev/null | wc -l')
[ "$n" = "4" ] && ok "4 children started" || bad "only $n children"

echo "[2] kill -9 taosd"
docker exec ep-normal-1 sh -c 'kill -9 $(cat /tmp/taosd.pid)'
wait_state ep-normal-1 "exited/137" 10 && ok "container exits 137" || bad "state=$(state ep-normal-1)"

echo "[3] kill -9 taosadapter (any child, not just taosd)"
docker rm -f ep-normal-2 >/dev/null 2>&1
docker run -d --name ep-normal-2 ep-normal >/dev/null
sleep 4
docker exec ep-normal-2 sh -c 'kill -9 $(cat /tmp/taosadapter.pid)'
wait_state ep-normal-2 "exited/137" 10 && ok "container exits 137" || bad "state=$(state ep-normal-2)"

echo "[4] taosd dies 1s into start-up"
docker rm -f ep-fail-1 >/dev/null 2>&1
start=$(date +%s)
docker run -d --name ep-fail-1 ep-fail >/dev/null
if wait_state ep-fail-1 "exited/1" 15; then
    elapsed=$(( $(date +%s) - start ))
    [ "$elapsed" -le 8 ] && ok "aborted in ${elapsed}s instead of burning the ready budget" \
                         || bad "took ${elapsed}s"
else
    bad "state=$(state ep-fail-1)"
fi
docker logs ep-fail-1 2>&1 | grep -q "exited during startup" \
    && ok "log names the dead process" || bad "log does not name the process"

echo "[5] docker stop shuts children down gracefully"
mkdir -p /tmp/entrypoint-test-logs && rm -f /tmp/entrypoint-test-logs/taosd.log
docker rm -f ep-slow-1 >/dev/null 2>&1
docker run -d --name ep-slow-1 -v /tmp/entrypoint-test-logs:/logs ep-slow >/dev/null
sleep 4
docker stop ep-slow-1 >/dev/null
[ "$(state ep-slow-1)" = "exited/0" ] && ok "exit code 0" || bad "state=$(state ep-slow-1)"
grep -q "flush done" /tmp/entrypoint-test-logs/taosd.log 2>/dev/null \
    && ok "taosd received SIGTERM and finished flushing" \
    || bad "taosd was killed before it could flush"

echo "[6] TAOS_DISABLE_ADAPTER=1"
docker rm -f ep-noep-1 >/dev/null 2>&1
docker run -d --name ep-noep-1 -e TAOS_DISABLE_ADAPTER=1 ep-normal >/dev/null
sleep 4
[ "$(state ep-noep-1)" = "running/0" ] && ok "normal start" || bad "state=$(state ep-noep-1)"
docker exec ep-noep-1 sh -c 'ls /tmp/taosadapter.pid' >/dev/null 2>&1 \
    && bad "adapter should not have started" || ok "adapter not started, not tracked"
docker exec ep-noep-1 sh -c 'kill -9 $(cat /tmp/taosd.pid)'
wait_state ep-noep-1 "exited/137" 10 && ok "taosd death still detected" || bad "state=$(state ep-noep-1)"

echo
echo "==== $pass passed / $fail failed ===="
exit "$fail"
