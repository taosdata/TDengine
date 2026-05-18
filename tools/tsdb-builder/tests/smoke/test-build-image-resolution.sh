#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="${ROOT}/tests/smoke/.tmp/test-build-image-resolution.$$"
mkdir -p "${TMP}"
trap 'rm -rf "${TMP}"' EXIT

DOCKER_LOG="${TMP}/docker.log"
BIN_DIR="${TMP}/bin"
FAKE_DOCKER="${BIN_DIR}/docker"
SRC_DIR="${TMP}/src"
CACHE_DIR="${TMP}/cache"

mkdir -p "${BIN_DIR}" "${SRC_DIR}" "${CACHE_DIR}"

cat > "${FAKE_DOCKER}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${DOCKER_LOG}"

has_local_image() {
  case " ${LOCAL_IMAGE_LIST:-} " in
    *" $1 "*) return 0 ;;
    *) return 1 ;;
  esac
}

case "${1:-}" in
  image)
    if [[ "${2:-}" == "inspect" ]]; then
      if has_local_image "${3:-}"; then
        exit 0
      fi
      exit 1
    fi
    exit 0
    ;;
  pull|run)
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
EOF
chmod +x "${FAKE_DOCKER}"

export DOCKER_LOG

run_script_case() {
  local name="$1"
  local script_path="$2"
  local local_images="$3"
  local expect_pull="$4"
  local expected_image="$5"
  shift 5

  : > "${DOCKER_LOG}"
  export LOCAL_IMAGE_LIST="${local_images}"

  if ! PATH="${BIN_DIR}:$PATH" bash "${ROOT}/${script_path}" "$@" >"${TMP}/run.out" 2>&1; then
    cat "${TMP}/run.out" >&2
    echo "${script_path} failed unexpectedly for case: ${name}" >&2
    exit 1
  fi

  grep -F -q "image inspect ${expected_image}" "${DOCKER_LOG}"

  if [[ "${expect_pull}" == "yes" ]]; then
    grep -F -q "pull ${expected_image}" "${DOCKER_LOG}"
  else
    if grep -F -q "pull ${expected_image}" "${DOCKER_LOG}"; then
      echo "expected ${name} to avoid pull for ${expected_image}" >&2
      exit 1
    fi
  fi

  grep -F -q "run --rm --platform=linux/" "${DOCKER_LOG}"
  grep -F -q "${expected_image}" "${DOCKER_LOG}"
}

run_script_case \
  "local latest core image" \
  "build.sh" \
  "harbor.tdengine.net/tsdb-builder/core:latest-amd64" \
  "no" \
  "harbor.tdengine.net/tsdb-builder/core:latest-amd64" \
  --src "${SRC_DIR}" --cache "${CACHE_DIR}" --arch amd64 --image core engine

run_script_case \
  "missing versioned core image" \
  "build.sh" \
  "" \
  "yes" \
  "harbor.tdengine.net/tsdb-builder/core:3.4.1-amd64" \
  --src "${SRC_DIR}" --cache "${CACHE_DIR}" --arch amd64 --image core:3.4.1 engine

run_script_case \
  "forced latest others pull" \
  "build.sh" \
  "harbor.tdengine.net/tsdb-builder/others:latest-arm64" \
  "yes" \
  "harbor.tdengine.net/tsdb-builder/others:latest-arm64" \
  --src "${SRC_DIR}" --cache "${CACHE_DIR}" --arch arm64 --pull-image --image others insight

: > "${DOCKER_LOG}"
if ! PATH="${BIN_DIR}:$PATH" bash "${ROOT}/verify-image.sh" core:amd64 >"${TMP}/verify.out" 2>&1; then
  cat "${TMP}/verify.out" >&2
  echo "verify-image.sh failed unexpectedly for shorthand latest image case" >&2
  exit 1
fi
grep -F -q "run --rm harbor.tdengine.net/tsdb-builder/core:latest-amd64 /usr/local/bin/verify-image.sh --in-container" "${DOCKER_LOG}"

echo "PASS"
