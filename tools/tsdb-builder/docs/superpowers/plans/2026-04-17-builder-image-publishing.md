# Builder Image Publishing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move builder image creation and consumption to fixed Harbor repositories with explicit versioned publishing, `latest` maintenance, and `build.sh --image core | others | core:3.4.1 | others:3.4.1` resolution.

**Architecture:** Keep the two existing image build entrypoints, but upgrade them from local-only tagging to Harbor-first publishing. `build.sh` becomes the single consumer of canonical Harbor-style tags by resolving `core` / `core:3.4.1` into exact single-arch image refs such as `:latest-amd64` and `:3.4.1-arm64`, using local images first and pulling only when needed.

**Tech Stack:** Bash 3.2-compatible shell scripts, Docker Buildx, Docker manifest commands, Markdown docs

---

## File structure and responsibilities

- Modify: `build-core-image.sh`
  - Parse `--version`
  - Build the core image for one architecture
  - Push version-and-arch tags such as `:3.4.1-amd64` and moving arch tags such as `:latest-amd64`
  - Create version manifests such as `:3.4.1` and the moving `:latest` manifest when both arch tags exist
- Modify: `build-others-image.sh`
  - Same behavior as `build-core-image.sh`, but for the `others` repository
- Modify: `build.sh`
  - Accept `--image core|others|core:3.4.1|others:3.4.1`
  - Add `--pull-image`
  - Resolve exact Harbor image refs such as `:latest-amd64` and `:3.4.1-arm64`
  - Prefer local image, pull on miss or force pull
- Create: `tests/smoke/test-image-publish-flow.sh`
  - Offline smoke test with a fake `docker` binary to validate publish and manifest command flow
- Create: `tests/smoke/test-build-image-selection.sh`
  - Offline smoke test with a fake `docker` binary to validate `build.sh` image resolution and pull behavior
- Modify: `README.md`
  - Update image build, publish, and `build.sh --image` docs
- Modify: `.github/copilot-instructions.md`
  - Sync operational guidance with the new image naming and pull flow

## Task 1: Add the image publish smoke test harness

**Files:**
- Create: `tests/smoke/test-image-publish-flow.sh`
- Test: `tests/smoke/test-image-publish-flow.sh`

- [ ] **Step 1: Write the failing smoke test**

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

DOCKER_LOG="${TMP}/docker.log"
FAKE_DOCKER="${TMP}/docker"
PKG_DIR="${TMP}/packages"
mkdir -p "${PKG_DIR}"

cat > "${FAKE_DOCKER}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${DOCKER_LOG}"
case "${1:-}" in
  buildx) exit 0 ;;
  push) exit 0 ;;
  manifest)
    if [[ "${2:-}" == "inspect" ]]; then
      case "${3:-}" in
        *:3.4.1-arm64|*:3.4.1-amd64) exit "${SIBLING_EXISTS:-1}" ;;
        *) exit 1 ;;
      esac
    fi
    exit 0
    ;;
  *) exit 0 ;;
esac
EOF
chmod +x "${FAKE_DOCKER}"

export DOCKER_LOG

if bash "${ROOT}/build-core-image.sh" --arch amd64 --packages "${PKG_DIR}" >"${TMP}/missing-version.out" 2>&1; then
  echo "expected build-core-image.sh to require --version"
  exit 1
fi
grep -q -- '--version' "${TMP}/missing-version.out"

SIBLING_EXISTS=1 DOCKER_BIN="${FAKE_DOCKER}" \
  bash "${ROOT}/build-core-image.sh" --arch amd64 --version 3.4.1 --packages "${PKG_DIR}"

grep -q 'buildx build .*harbor.tdengine.net/tsdb-builder/core:3.4.1-amd64' "${DOCKER_LOG}"
grep -q 'push harbor.tdengine.net/tsdb-builder/core:latest-amd64' "${DOCKER_LOG}"
grep -q 'manifest create harbor.tdengine.net/tsdb-builder/core:3.4.1' "${DOCKER_LOG}"
grep -q 'manifest create harbor.tdengine.net/tsdb-builder/core:latest' "${DOCKER_LOG}"
```

- [ ] **Step 2: Run the smoke test and verify it fails against the current script**

Run:

```bash
bash tests/smoke/test-image-publish-flow.sh
```

Expected: FAIL because `build-core-image.sh` currently accepts no `--version`, still tags `tsdb-builder-core:amd64`, and never runs any push or manifest commands.

- [ ] **Step 3: Tighten the fake docker harness so both scripts can reuse it**

Update the fake docker stub so it can distinguish `core` and `others` repos using one env var:

```bash
TARGET_REPO="${TARGET_REPO:-harbor.tdengine.net/tsdb-builder/core}"

case "${1:-}" in
  manifest)
    if [[ "${2:-}" == "inspect" ]]; then
      case "${3:-}" in
        "${TARGET_REPO}:3.4.1-amd64"|\
        "${TARGET_REPO}:3.4.1-arm64") exit "${SIBLING_EXISTS:-1}" ;;
        *) exit 1 ;;
      esac
    fi
    exit 0
    ;;
esac
```

- [ ] **Step 4: Re-run the smoke test to confirm it still fails for the right reason**

Run:

```bash
bash tests/smoke/test-image-publish-flow.sh
```

Expected: FAIL on missing Harbor tags / missing `--version` handling, not because the test harness itself is broken.

- [ ] **Step 5: Commit the test harness**

```bash
git add tests/smoke/test-image-publish-flow.sh
git commit -m "test: add smoke test for image publish flow"
```

## Task 2: Implement Harbor publishing in both image build scripts

**Files:**
- Modify: `build-core-image.sh`
- Modify: `build-others-image.sh`
- Test: `tests/smoke/test-image-publish-flow.sh`

- [ ] **Step 1: Add required `--version` parsing and repository constants to `build-core-image.sh`**

Insert the new defaults and parse state near the top:

```bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_ARGS_FILE="${SCRIPT_DIR}/.build-args"
ARCH="amd64"
VERSION=""
PACKAGES_DIR="${HOME}/packages"
DOCKER_BIN="${DOCKER_BIN:-docker}"
REPOSITORY="harbor.tdengine.net/tsdb-builder/core"
```

Extend the parser:

```bash
        --version|-v)
            if [[ $# -lt 2 ]]; then echo "ERROR: --version requires an argument"; exit 1; fi
            VERSION="$2"; shift 2 ;;
```

Require it after arch validation:

```bash
if [[ -z "${VERSION}" ]]; then
    echo "ERROR: --version is required."
    echo "Usage: $0 [--arch amd64|arm64] --version 3.4.1 [--packages /path/to/packages]"
    exit 1
fi
```

- [ ] **Step 2: Replace the old local-only tag with canonical Harbor arch tags in `build-core-image.sh`**

Add derived tags before the build command:

```bash
VERSION_TAG="${REPOSITORY}:${VERSION}-${ARCH}"
LATEST_ARCH_TAG="${REPOSITORY}:latest-${ARCH}"
```

Update the build to tag both refs:

```bash
DOCKER_BUILDKIT=1 "${DOCKER_BIN}" buildx build \
    --platform "linux/${ARCH}" \
    $build_args \
    --build-context packages="${PACKAGES_DIR}" \
    --tag "${VERSION_TAG}" \
    --tag "${LATEST_ARCH_TAG}" \
    --load \
    -f "${SCRIPT_DIR}/Dockerfile.core" \
    "${SCRIPT_DIR}"
```

- [ ] **Step 3: Add push and best-effort manifest maintenance to `build-core-image.sh`**

Add the helper block after the build:

```bash
push_or_die() {
    local image_ref="$1"
    if ! "${DOCKER_BIN}" push "${image_ref}"; then
        echo "ERROR: Failed to push ${image_ref}"
        echo "Run: docker login harbor.tdengine.net"
        exit 1
    fi
}

other_arch() {
    if [[ "$1" == "amd64" ]]; then
        echo "arm64"
    else
        echo "amd64"
    fi
}

create_manifest_if_ready() {
    local sibling_arch sibling_tag version_manifest latest_manifest
    sibling_arch="$(other_arch "${ARCH}")"
    sibling_tag="${REPOSITORY}:${VERSION}-${sibling_arch}"
    version_manifest="${REPOSITORY}:${VERSION}"
    latest_manifest="${REPOSITORY}:latest"

    if ! "${DOCKER_BIN}" manifest inspect "${sibling_tag}" >/dev/null 2>&1; then
        echo "[INFO] Sibling image not found yet: ${sibling_tag}"
        echo "[INFO] Skipping manifest update for ${version_manifest} and ${latest_manifest}"
        return 0
    fi

    "${DOCKER_BIN}" manifest create "${version_manifest}" \
        "${REPOSITORY}:${VERSION}-amd64" \
        "${REPOSITORY}:${VERSION}-arm64"
    "${DOCKER_BIN}" manifest push --purge "${version_manifest}"

    "${DOCKER_BIN}" manifest create "${latest_manifest}" \
        "${REPOSITORY}:latest-amd64" \
        "${REPOSITORY}:latest-arm64"
    "${DOCKER_BIN}" manifest push --purge "${latest_manifest}"
}

push_or_die "${VERSION_TAG}"
push_or_die "${LATEST_ARCH_TAG}"
create_manifest_if_ready
```

- [ ] **Step 4: Apply the `others` implementation explicitly in `build-others-image.sh`**

Add the same parser and helper state, but with the `others` repository:

```bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_ARGS_FILE="${SCRIPT_DIR}/.build-args"
ARCH="amd64"
VERSION=""
PACKAGES_DIR="${HOME}/packages"
DOCKER_BIN="${DOCKER_BIN:-docker}"
REPOSITORY="harbor.tdengine.net/tsdb-builder/others"
VERSION_TAG="${REPOSITORY}:${VERSION}-${ARCH}"
LATEST_ARCH_TAG="${REPOSITORY}:latest-${ARCH}"
```

Use the `others` Dockerfile in the build command:

```bash
DOCKER_BUILDKIT=1 "${DOCKER_BIN}" buildx build \
    --platform "linux/${ARCH}" \
    $build_args \
    --build-context packages="${PACKAGES_DIR}" \
    --tag "${VERSION_TAG}" \
    --tag "${LATEST_ARCH_TAG}" \
    --load \
    -f "${SCRIPT_DIR}/Dockerfile.others" \
    "${SCRIPT_DIR}"
```

Add the full helper block below after the `others` build:

```bash
push_or_die() {
    local image_ref="$1"
    if ! "${DOCKER_BIN}" push "${image_ref}"; then
        echo "ERROR: Failed to push ${image_ref}"
        echo "Run: docker login harbor.tdengine.net"
        exit 1
    fi
}

other_arch() {
    if [[ "$1" == "amd64" ]]; then
        echo "arm64"
    else
        echo "amd64"
    fi
}

create_manifest_if_ready() {
    local sibling_arch sibling_tag version_manifest latest_manifest
    sibling_arch="$(other_arch "${ARCH}")"
    sibling_tag="${REPOSITORY}:${VERSION}-${sibling_arch}"
    version_manifest="${REPOSITORY}:${VERSION}"
    latest_manifest="${REPOSITORY}:latest"

    if ! "${DOCKER_BIN}" manifest inspect "${sibling_tag}" >/dev/null 2>&1; then
        echo "[INFO] Sibling image not found yet: ${sibling_tag}"
        echo "[INFO] Skipping manifest update for ${version_manifest} and ${latest_manifest}"
        return 0
    fi

    "${DOCKER_BIN}" manifest create "${version_manifest}" \
        "${REPOSITORY}:${VERSION}-amd64" \
        "${REPOSITORY}:${VERSION}-arm64"
    "${DOCKER_BIN}" manifest push --purge "${version_manifest}"

    "${DOCKER_BIN}" manifest create "${latest_manifest}" \
        "${REPOSITORY}:latest-amd64" \
        "${REPOSITORY}:latest-arm64"
    "${DOCKER_BIN}" manifest push --purge "${latest_manifest}"
}

push_or_die "${VERSION_TAG}"
push_or_die "${LATEST_ARCH_TAG}"
create_manifest_if_ready
```

- [ ] **Step 5: Re-run the publish smoke test**

Run:

```bash
bash tests/smoke/test-image-publish-flow.sh
```

Expected: PASS for the `core` publish flow. Then duplicate the last invocation in the test script for `build-others-image.sh` by setting:

```bash
TARGET_REPO="harbor.tdengine.net/tsdb-builder/others"
```

and confirm the `others` flow also passes.

- [ ] **Step 6: Commit the script changes**

```bash
git add build-core-image.sh build-others-image.sh tests/smoke/test-image-publish-flow.sh
git commit -m "feat: publish builder images to Harbor"
```

## Task 3: Teach `build.sh` to resolve canonical image refs

**Files:**
- Create: `tests/smoke/test-build-image-selection.sh`
- Modify: `build.sh`
- Test: `tests/smoke/test-build-image-selection.sh`

- [ ] **Step 1: Write the failing `build.sh` smoke test**

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

DOCKER_LOG="${TMP}/docker.log"
FAKE_DOCKER="${TMP}/docker"
cat > "${FAKE_DOCKER}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${DOCKER_LOG}"
case "${1:-}" in
  image)
    if [[ "${2:-}" == "inspect" && "${IMAGE_PRESENT:-0}" == "1" ]]; then
      exit 0
    fi
    exit 1
    ;;
  pull|run) exit 0 ;;
  *) exit 0 ;;
esac
EOF
chmod +x "${FAKE_DOCKER}"
export DOCKER_LOG

IMAGE_PRESENT=0 DOCKER_BIN="${FAKE_DOCKER}" \
  bash "${ROOT}/build.sh" --image core:3.4.1 --arch arm64 --pull-image engine -DCMAKE_BUILD_TYPE=Debug >"${TMP}/build.out" 2>&1 || true

grep -q 'harbor.tdengine.net/tsdb-builder/core:3.4.1-arm64' "${TMP}/build.out"
grep -q 'pull harbor.tdengine.net/tsdb-builder/core:3.4.1-arm64' "${DOCKER_LOG}"
```

- [ ] **Step 2: Run the smoke test and confirm it fails**

Run:

```bash
bash tests/smoke/test-build-image-selection.sh
```

Expected: FAIL because `build.sh` currently rejects `--image core:3.4.1`, has no `--pull-image`, and still resolves `tsdb-builder-core:${ARCH}`.

- [ ] **Step 3: Add new selector parsing and `--pull-image` to `build.sh`**

Near the defaults:

```bash
PULL_IMAGE=false
IMAGE_SELECTOR=""
BUILDER_REGISTRY="harbor.tdengine.net/tsdb-builder"
DOCKER_BIN="${DOCKER_BIN:-docker}"
```

Parse the new flag:

```bash
        --pull-image)
            PULL_IMAGE=true
            shift
            ;;
```

Replace the old `IMAGE_OVERRIDE` validation with selector parsing:

```bash
parse_image_selector() {
    case "${IMAGE_SELECTOR}" in
        core|others)
            USE_IMAGE="${IMAGE_SELECTOR}"
            IMAGE_TAG="latest"
            ;;
        core:*|others:*)
            USE_IMAGE="${IMAGE_SELECTOR%%:*}"
            IMAGE_TAG="${IMAGE_SELECTOR#*:}"
            ;;
        *)
            echo "ERROR: Invalid image '${IMAGE_SELECTOR}'. Use core, others, core:3.4.1, or others:3.4.1."
            exit 1
            ;;
    esac
}
```

and set:

```bash
IMAGE_REPO="${BUILDER_REGISTRY}/${USE_IMAGE}"
IMAGE="${IMAGE_REPO}:${IMAGE_TAG}-${ARCH}"
```

- [ ] **Step 4: Add local-first / pull-first behavior in `build.sh`**

Before the final `docker run`, insert:

```bash
ensure_image_available() {
    if [[ "${PULL_IMAGE}" == "false" ]] && "${DOCKER_BIN}" image inspect "${IMAGE}" >/dev/null 2>&1; then
        echo "[INFO] Using local image: ${IMAGE}"
        return 0
    fi

    echo "[INFO] Pulling image: ${IMAGE}"
    if ! "${DOCKER_BIN}" pull "${IMAGE}"; then
        echo "ERROR: Failed to pull ${IMAGE}"
        echo "Run: docker login harbor.tdengine.net"
        exit 1
    fi
}

ensure_image_available
```

and replace the final `docker run` call with:

```bash
"${DOCKER_BIN}" run "${DOCKER_MAIN_ARGS[@]}" bash -c "$CONTAINER_SCRIPT"
```

Also change any earlier hard-coded `docker` uses in this file to `"${DOCKER_BIN}"`.

- [ ] **Step 5: Update usage text and logging in `build.sh`**

Replace the key usage/help strings with:

```bash
Usage: ./build.sh --image core|others|core:3.4.1|others:3.4.1 [--arch amd64|arm64] [--src PATH]
                  [--cache PATH] [--clean] [--pull-image] [component...] [-DKEY=VALUE ...]
```

and add log lines:

```bash
echo "[INFO] Image selector: ${IMAGE_SELECTOR}"
echo "[INFO] Resolved image: ${IMAGE}"
```

- [ ] **Step 6: Re-run the `build.sh` smoke test**

Run:

```bash
bash tests/smoke/test-build-image-selection.sh
```

Expected: PASS, with the log showing `harbor.tdengine.net/tsdb-builder/core:3.4.1-arm64` and the fake docker log showing a `pull` before `run`.

- [ ] **Step 7: Commit the `build.sh` integration**

```bash
git add build.sh tests/smoke/test-build-image-selection.sh
git commit -m "feat: resolve builder images from Harbor tags"
```

## Task 4: Update README and Copilot instructions

**Files:**
- Modify: `README.md`
- Modify: `.github/copilot-instructions.md`
- Test: `README.md`
- Test: `.github/copilot-instructions.md`

- [ ] **Step 1: Update README image build and publish examples**

Replace the image build quick-start examples with versioned publish examples:

```bash
./build-core-image.sh --version 3.4.1
./build-core-image.sh --arch arm64 --version 3.4.1
./build-others-image.sh --version 3.4.1
./build-others-image.sh --arch arm64 --version 3.4.1
```

Add the canonical tag examples:

```text
harbor.tdengine.net/tsdb-builder/core:3.4.1-amd64
harbor.tdengine.net/tsdb-builder/core:3.4.1-arm64
harbor.tdengine.net/tsdb-builder/core:3.4.1
harbor.tdengine.net/tsdb-builder/core:latest-amd64
harbor.tdengine.net/tsdb-builder/core:latest-arm64
harbor.tdengine.net/tsdb-builder/core:latest
```

- [ ] **Step 2: Update README `build.sh` examples**

Replace the old examples with:

```bash
./build.sh --image core engine taosx
./build.sh --image core:3.4.1 engine taosx
./build.sh --image others --pull-image explorer-ui insight jdbc
./build.sh --image others:3.4.1 --arch arm64 rust
```

Add a short note:

```text
build.sh first checks for the exact single-arch image locally (for example,
harbor.tdengine.net/tsdb-builder/core:latest-amd64). If it is absent, it pulls
that exact tag from Harbor. Use --pull-image to force refresh.
```

- [ ] **Step 3: Update `.github/copilot-instructions.md` operational guidance**

Replace the image build section with:

```bash
./build-core-image.sh --version 3.4.1 [--arch amd64|arm64] [--packages /path/to/packages]
./build-others-image.sh --version 3.4.1 [--arch amd64|arm64] [--packages /path/to/packages]
```

Add a short architecture note:

```text
Published tags are versioned and architecture-specific (`:3.4.1-amd64`,
`:3.4.1-arm64`) plus multi-arch manifests (`:3.4.1`, `:latest`).
build.sh resolves `--image core` / `core:3.4.1` / `others` / `others:3.4.1` to exact
single-arch tags and can force refresh with `--pull-image`.
```

- [ ] **Step 4: Run documentation and script syntax checks**

Run:

```bash
bash -n build-core-image.sh
bash -n build-others-image.sh
bash -n build.sh
grep -n 'latest-amd64\|core:3.4.1\|--pull-image' README.md .github/copilot-instructions.md
```

Expected: no bash syntax errors; the grep output shows the new image naming and `--pull-image` docs in both files.

- [ ] **Step 5: Commit the docs update**

```bash
git add README.md .github/copilot-instructions.md
git commit -m "docs: document Harbor builder image workflow"
```

## Task 5: Final smoke verification and cleanup

**Files:**
- Modify: `build-core-image.sh`
- Modify: `build-others-image.sh`
- Modify: `build.sh`
- Modify: `README.md`
- Modify: `.github/copilot-instructions.md`
- Test: `tests/smoke/test-image-publish-flow.sh`
- Test: `tests/smoke/test-build-image-selection.sh`

- [ ] **Step 1: Run both smoke tests back-to-back**

```bash
bash tests/smoke/test-image-publish-flow.sh
bash tests/smoke/test-build-image-selection.sh
```

Expected: both PASS.

- [ ] **Step 2: Run the shell syntax checks again**

```bash
bash -n build-core-image.sh
bash -n build-others-image.sh
bash -n build.sh
```

Expected: all commands exit 0.

- [ ] **Step 3: Inspect the final diff for scope**

```bash
git status --short
git --no-pager diff -- build-core-image.sh build-others-image.sh build.sh README.md .github/copilot-instructions.md tests/smoke/test-image-publish-flow.sh tests/smoke/test-build-image-selection.sh
```

Expected: `git status --short` shows only the planned shell scripts, docs, and smoke tests as pending fixups, and the detailed diff contains no unrelated files.

- [ ] **Step 4: Create the final feature commit if any fixups remain**

```bash
git add build-core-image.sh build-others-image.sh build.sh README.md .github/copilot-instructions.md tests/smoke/test-image-publish-flow.sh tests/smoke/test-build-image-selection.sh
git commit -m "chore: finalize builder image Harbor workflow"
```

- [ ] **Step 5: Push the branch**

```bash
git push
```
