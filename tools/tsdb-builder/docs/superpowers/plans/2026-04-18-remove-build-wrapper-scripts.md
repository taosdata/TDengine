# Remove Build Wrapper Scripts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete `build-core.sh` and `build-others.sh`, replace their active documentation with `build.sh` equivalents, and prove the supported `build.sh` commands cover the removed workflows.

**Architecture:** Keep `build.sh` as the single supported compilation entrypoint. Add one focused smoke test that treats wrapper removal as a product behavior change: the wrappers must be gone, active docs must point to `build.sh`, and the documented replacement commands must still resolve and launch correctly under a fake Docker binary.

**Tech Stack:** Bash, ripgrep, existing smoke-test pattern with fake `docker`, Markdown docs

---

## File map

- Delete: `build-core.sh` — deprecated core-only wrapper superseded by `build.sh`
- Delete: `build-others.sh` — deprecated others-only wrapper superseded by `build.sh`
- Create: `tests/smoke/test-build-wrapper-removal.sh` — focused regression/smoke test for wrapper removal and replacement coverage
- Modify: `README.md` — remove wrapper script guidance, keep only `build.sh` replacement commands
- Modify: `.github/copilot-instructions.md` — same cleanup for active agent guidance

### Task 1: Add the failing regression smoke test

**Files:**
- Create: `tests/smoke/test-build-wrapper-removal.sh`
- Modify: `tests/smoke/test-build-wrapper-removal.sh` (same file during this task)
- Test: `tests/smoke/test-build-wrapper-removal.sh`

- [ ] **Step 1: Write the failing test**

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="${ROOT}/tests/smoke/.tmp/test-build-wrapper-removal.$$"
mkdir -p "${TMP}"
trap 'rm -rf "${TMP}"' EXIT

DOCKER_LOG="${TMP}/docker.log"
BIN_DIR="${TMP}/bin"
mkdir -p "${BIN_DIR}"

cat > "${BIN_DIR}/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${DOCKER_LOG}"
case "${1:-}" in
  image) exit 0 ;;
  pull|run) exit 0 ;;
  *) exit 0 ;;
esac
EOF
chmod +x "${BIN_DIR}/docker"
export DOCKER_LOG

test ! -e "${ROOT}/build-core.sh"
test ! -e "${ROOT}/build-others.sh"

! rg -n "build-core\\.sh|build-others\\.sh" "${ROOT}/README.md" "${ROOT}/.github/copilot-instructions.md" >/dev/null

run_case() {
  local src_dir="$1"
  local cache_dir="$2"
  local expected_image="$3"
  local expected_output="$4"
  shift 4

  mkdir -p "${src_dir}" "${cache_dir}"
  PATH="${BIN_DIR}:$PATH" bash "${ROOT}/build.sh" --src "${src_dir}" --cache "${cache_dir}" "$@" >/dev/null 2>&1
  grep -F -q "${expected_image}" "${DOCKER_LOG}"
  grep -F -q "[INFO] Output      : ${src_dir}/${expected_output}" "${src_dir}/build.log"
}

run_case \
  "${TMP}/core-src" \
  "${TMP}/core-cache" \
  "harbor.tdengine.net/tsdb-builder/core:latest-amd64" \
  "debug/" \
  --arch amd64 --image core --clean core-all

run_case \
  "${TMP}/others-src" \
  "${TMP}/others-cache" \
  "harbor.tdengine.net/tsdb-builder/others:latest-amd64" \
  "debug-others/" \
  --arch amd64 --image others --clean others-all -DBUILD_TAOSX=OFF -DBUILD_ODBC=OFF

echo "PASS"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash tests/smoke/test-build-wrapper-removal.sh`
Expected: FAIL because `build-core.sh` and `build-others.sh` still exist and active docs still mention them.

- [ ] **Step 3: Commit the failing test**

```bash
git add tests/smoke/test-build-wrapper-removal.sh
git commit -m "test: add wrapper removal regression"
```

### Task 2: Remove the wrappers and clean active docs

**Files:**
- Delete: `build-core.sh`
- Delete: `build-others.sh`
- Modify: `README.md`
- Modify: `.github/copilot-instructions.md`
- Test: `tests/smoke/test-build-wrapper-removal.sh`

- [ ] **Step 1: Delete the legacy scripts**

```bash
rm build-core.sh
rm build-others.sh
```

- [ ] **Step 2: Update README to remove wrapper references and keep build.sh replacements explicit**

```md
#### 全量编译（CI 等效命令）

```bash
# 全部 core 组件
./build.sh --image core --clean core-all

# 全部 others 组件（TAOSX Rust 二进制由 core 步骤产出，ODBC 仍在 CI 路径中关闭）
./build.sh --image others --clean others-all -DBUILD_TAOSX=OFF -DBUILD_ODBC=OFF
```
```

- [ ] **Step 3: Update `.github/copilot-instructions.md` to match**

```md
### Compile TSDB components (CI / full build)
```bash
# Full core build — clean, all core components
./build.sh --image core --clean core-all

# Full others build — TAOSX Rust binary is produced by the preceding core step;
# ODBC is excluded from CI builds
./build.sh --image others --clean others-all -DBUILD_TAOSX=OFF -DBUILD_ODBC=OFF
```
```

- [ ] **Step 4: Run the regression test to verify it passes**

Run: `bash tests/smoke/test-build-wrapper-removal.sh`
Expected: PASS

- [ ] **Step 5: Commit the removal**

```bash
git add README.md .github/copilot-instructions.md tests/smoke/test-build-wrapper-removal.sh
git rm build-core.sh build-others.sh
git commit -m "refactor: remove deprecated build wrappers"
```

### Task 3: Run repository verification and prepare handoff

**Files:**
- Test: `tests/smoke/test-image-publish-flow.sh`
- Test: `tests/smoke/test-build-image-resolution.sh`
- Test: `tests/smoke/test-build-wrapper-removal.sh`
- Modify: none unless a verification failure requires a targeted fix

- [ ] **Step 1: Run the smoke suite**

Run:

```bash
bash tests/smoke/test-image-publish-flow.sh
bash tests/smoke/test-build-image-resolution.sh
bash tests/smoke/test-build-wrapper-removal.sh
```

Expected: all three print `PASS`

- [ ] **Step 2: Run shell syntax checks**

Run:

```bash
bash -n build.sh verify-image.sh build-core-image.sh build-others-image.sh \
  tests/smoke/test-image-publish-flow.sh \
  tests/smoke/test-build-image-resolution.sh \
  tests/smoke/test-build-wrapper-removal.sh
```

Expected: no output, exit code 0

- [ ] **Step 3: Run diff sanity checks**

Run:

```bash
git --no-pager diff --check
git --no-pager status --short
```

Expected: `diff --check` prints nothing; `status --short` shows only the intended wrapper-removal changes

- [ ] **Step 4: Commit only if verification required a targeted follow-up fix**

```bash
git add -A
git commit -m "test: finalize build wrapper removal verification"
```
