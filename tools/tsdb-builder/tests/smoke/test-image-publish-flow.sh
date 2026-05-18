#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="${ROOT}/tests/smoke/.tmp/test-image-publish-flow.$$"
mkdir -p "${TMP}"
trap 'rm -rf "${TMP}"' EXIT

DOCKER_LOG="${TMP}/docker.log"
BIN_DIR="${TMP}/bin"
FAKE_DOCKER="${BIN_DIR}/docker"
PKG_DIR="${TMP}/packages"
mkdir -p "${PKG_DIR}" "${BIN_DIR}"

cat > "${FAKE_DOCKER}" <<'EO2F'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${DOCKER_LOG}"
TARGET_REPO="${TARGET_REPO:-harbor.tdengine.net/tsdb-builder/core}"
case "${1:-}" in
  buildx) exit 0 ;;
  push) exit 0 ;;
  manifest)
    if [[ "${2:-}" == "inspect" ]]; then
      case "${3:-}" in
        "${TARGET_REPO}:3.4.1-amd64"|\
        "${TARGET_REPO}:3.4.1-arm64")
          # SIBLING_EXISTS semantics: set to 1 to indicate sibling exists (exit 0), unset/0 means missing (exit 1)
          if [[ "${SIBLING_EXISTS:-0}" -eq 1 ]]; then
            exit 0
          else
            exit 1
          fi
          ;;
        *)
          # Inspect for any other ref should fail
          exit 1
          ;;
      esac
    fi
    if [[ "${2:-}" == "create" && "${FAIL_MANIFEST_CREATE:-0}" -eq 1 ]]; then
      exit 1
    fi
    if [[ "${2:-}" == "push" && "${FAIL_MANIFEST_PUSH:-0}" -eq 1 ]]; then
      exit 1
    fi
    exit 0
    ;;
  *) exit 0 ;;
esac
EO2F
chmod +x "${FAKE_DOCKER}"

export DOCKER_LOG

run_case() {
  local target_repo="$1"
  local target_script="$2"
  local fail_manifest_mode="${3:-none}"
  local sibling_arch

  if [[ "${target_script}" == "build-core-image.sh" || "${target_script}" == "build-others-image.sh" ]]; then
    sibling_arch="arm64"
  fi

  export TARGET_REPO="${target_repo}"

  if PATH="${BIN_DIR}:$PATH" bash "${ROOT}/${target_script}" --arch amd64 --packages "${PKG_DIR}" >"${TMP}/missing-version.out" 2>&1; then
    echo "expected ${target_script} to require --version"
    exit 1
  fi
  grep -q -- '--version' "${TMP}/missing-version.out"

  # Clear docker log between phases so early failures can't pollute the success-path assertions
  : > "${DOCKER_LOG}"

  case "${fail_manifest_mode}" in
    none)
      SIBLING_EXISTS=1 PATH="${BIN_DIR}:$PATH" \
        bash "${ROOT}/${target_script}" --arch amd64 --version 3.4.1 --packages "${PKG_DIR}" >"${TMP}/run.out" 2>&1
      ;;
    create)
      SIBLING_EXISTS=1 FAIL_MANIFEST_CREATE=1 PATH="${BIN_DIR}:$PATH" \
        bash "${ROOT}/${target_script}" --arch amd64 --version 3.4.1 --packages "${PKG_DIR}" >"${TMP}/run.out" 2>&1
      ;;
    push)
      SIBLING_EXISTS=1 FAIL_MANIFEST_PUSH=1 PATH="${BIN_DIR}:$PATH" \
        bash "${ROOT}/${target_script}" --arch amd64 --version 3.4.1 --packages "${PKG_DIR}" >"${TMP}/run.out" 2>&1
      ;;
    *)
      echo "unknown fail_manifest_mode: ${fail_manifest_mode}" >&2
      exit 1
      ;;
  esac

  # Require buildx build that includes the amd64 tag for 3.4.1
  if ! grep -F "buildx build" "${DOCKER_LOG}" | grep -F -q "${target_repo}:3.4.1-amd64"; then
    echo "expected buildx build for ${target_repo}:3.4.1-amd64" >&2
    exit 1
  fi
  # Negative check: ensure buildx lines do not include unsuffixed canonical tags for 3.4.1
  if grep -F "buildx build" "${DOCKER_LOG}" | sed -e "s|${target_repo}:3.4.1-amd64|SAFE_AMD64|g" -e "s|${target_repo}:3.4.1-arm64|SAFE_ARM64|g" | grep -F -q "${target_repo}:3.4.1"; then
    echo "unexpected unsuffixed canonical tag ${target_repo}:3.4.1 present in buildx command" >&2
    exit 1
  fi
  # Negative check: ensure buildx lines do not include unsuffixed canonical tags for latest
  if grep -F "buildx build" "${DOCKER_LOG}" | sed -e "s|${target_repo}:latest-amd64|SAFE_AMD64|g" -e "s|${target_repo}:latest-arm64|SAFE_ARM64|g" | grep -F -q "${target_repo}:latest"; then
    echo "unexpected unsuffixed canonical tag ${target_repo}:latest present in buildx command" >&2
    exit 1
  fi
  # require push of the 3.4.1 amd64 tag
  grep -F -q "push ${target_repo}:3.4.1-amd64" "${DOCKER_LOG}"
  # require push of the latest amd64 tag
  grep -F -q "push ${target_repo}:latest-amd64" "${DOCKER_LOG}"
  # Negative assertions: canonical unsuffixed tags must NOT be pushed directly
  # Fail if a token-equal push of the canonical unsuffixed tag occurred (avoid matching suffixed tags)
  if awk -v repo="${target_repo}" 'BEGIN{found=0}
  { for(i=1;i<NF;i++) { if($i=="push" && $(i+1)==repo":3.4.1") found=1 } }
  END{ if(found) exit 0; exit 1 }' "${DOCKER_LOG}"; then
    echo "unexpected direct push of canonical tag ${target_repo}:3.4.1" >&2
    exit 1
  fi
  if awk -v repo="${target_repo}" 'BEGIN{found=0}
  { for(i=1;i<NF;i++) { if($i=="push" && $(i+1)==repo":latest") found=1 } }
  END{ if(found) exit 0; exit 1 }' "${DOCKER_LOG}"; then
    echo "unexpected direct push of canonical tag ${target_repo}:latest" >&2
    exit 1
  fi
  # Ensure the script checked only the opposite-arch sibling tag before creating manifests
  grep -F -q "manifest inspect ${target_repo}:3.4.1-${sibling_arch}" "${DOCKER_LOG}"

  # manifest create for 3.4.1 must be a single line that includes both arch refs (match exact target with trailing space)
  if ! grep -F "manifest create ${target_repo}:3.4.1 " "${DOCKER_LOG}" | grep -F "${target_repo}:3.4.1-amd64" | grep -F -q "${target_repo}:3.4.1-arm64"; then
    echo "expected single manifest create line for ${target_repo}:3.4.1 including both amd64 and arm64" >&2
    exit 1
  fi
  # manifest create for latest must be a single line that includes both arch refs (match exact target with trailing space)
  if ! grep -F "manifest create ${target_repo}:latest " "${DOCKER_LOG}" | grep -F "${target_repo}:latest-amd64" | grep -F -q "${target_repo}:latest-arm64"; then
    echo "expected single manifest create line for ${target_repo}:latest including both amd64 and arm64" >&2
    exit 1
  fi

  case "${fail_manifest_mode}" in
    none)
      grep -F -q "manifest rm ${target_repo}:3.4.1" "${DOCKER_LOG}"
      grep -F -q "manifest rm ${target_repo}:latest" "${DOCKER_LOG}"
      grep -F -q "manifest push --purge ${target_repo}:3.4.1" "${DOCKER_LOG}"
      grep -F -q "manifest push --purge ${target_repo}:latest" "${DOCKER_LOG}"
      ;;
    create)
      grep -F -q "manifest rm ${target_repo}:3.4.1" "${DOCKER_LOG}"
      grep -F -q "manifest rm ${target_repo}:latest" "${DOCKER_LOG}"
      grep -F -q "[WARN] Failed to create manifest ${target_repo}:3.4.1" "${TMP}/run.out"
      grep -F -q "[WARN] Failed to create manifest ${target_repo}:latest" "${TMP}/run.out"
      ;;
    push)
      grep -F -q "manifest rm ${target_repo}:3.4.1" "${DOCKER_LOG}"
      grep -F -q "manifest rm ${target_repo}:latest" "${DOCKER_LOG}"
      grep -F -q "manifest push --purge ${target_repo}:3.4.1" "${DOCKER_LOG}"
      grep -F -q "manifest push --purge ${target_repo}:latest" "${DOCKER_LOG}"
      grep -F -q "[WARN] Failed to push manifest ${target_repo}:3.4.1" "${TMP}/run.out"
      grep -F -q "[WARN] Failed to push manifest ${target_repo}:latest" "${TMP}/run.out"
      ;;
  esac
}

run_case "harbor.tdengine.net/tsdb-builder/core" "build-core-image.sh"
run_case "harbor.tdengine.net/tsdb-builder/others" "build-others-image.sh"
run_case "harbor.tdengine.net/tsdb-builder/core" "build-core-image.sh" create
run_case "harbor.tdengine.net/tsdb-builder/others" "build-others-image.sh" push

echo "PASS"
