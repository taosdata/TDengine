#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TARGET_SCRIPT="${REPO_ROOT}/.github/scripts/stop_services_and_collect_coverage.sh"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "${TMPDIR}"' EXIT

export CONTAINER_ID="fake-container"
export COVERAGE_BASE_DIR="${TMPDIR}/coverage"
export DIR_PATH="3683"
export FAKE_CONTAINER_DIR="${TMPDIR}/container"
export FAKE_COVERAGE_DIR="${COVERAGE_BASE_DIR}/${DIR_PATH}"
export FAKE_STATE_DIR="${TMPDIR}/state"

mkdir -p "${TMPDIR}/bin" "${FAKE_CONTAINER_DIR}/tmp" "${FAKE_COVERAGE_DIR}" "${FAKE_STATE_DIR}"

cat > "${TMPDIR}/bin/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

case "$1" in
  exec)
    shift
    container_id="$1"
    shift
    if [[ "$1" == "sh" ]]; then
      cat >/dev/null || true
      (
        sleep 3
        touch "${FAKE_COVERAGE_DIR}/coverage-111-1.profraw"
      ) &
      echo "$!" > "${FAKE_STATE_DIR}/profraw_writer.pid"
      exit 0
    fi

    if [[ "$1" == "/usr/local/bin/convert-coverage.sh" ]]; then
      coverage_dir="$2"
      output_path="$3"
      if compgen -G "${coverage_dir}/coverage-*.profraw" >/dev/null; then
        mkdir -p "$(dirname "${FAKE_CONTAINER_DIR}${output_path}")"
        printf 'TN:\nSF:fake.rs\nDA:1,1\nend_of_record\n' > "${FAKE_CONTAINER_DIR}${output_path}"
      else
        echo "Warning: No .profraw files found in ${coverage_dir}"
      fi
      exit 0
    fi

    echo "unsupported docker exec invocation: ${container_id} $*" >&2
    exit 1
    ;;
  cp)
    shift
    src="$1"
    dest="$2"
    container_path="${src#*:}"
    if [[ -f "${FAKE_CONTAINER_DIR}${container_path}" ]]; then
      cp "${FAKE_CONTAINER_DIR}${container_path}" "${dest}"
      exit 0
    fi
    exit 1
    ;;
  *)
    echo "unsupported docker invocation: $*" >&2
    exit 1
    ;;
esac
SH
chmod +x "${TMPDIR}/bin/docker"

PATH="${TMPDIR}/bin:${PATH}" "${TARGET_SCRIPT}"

if [[ -f "${FAKE_STATE_DIR}/profraw_writer.pid" ]]; then
  writer_pid="$(cat "${FAKE_STATE_DIR}/profraw_writer.pid")"
  wait "${writer_pid}" 2>/dev/null || true
fi

OUTPUT_FILE="${FAKE_COVERAGE_DIR}/llvm-cov-integration.lcov"
if [[ ! -f "${OUTPUT_FILE}" ]]; then
  echo "expected integration coverage file to be created: ${OUTPUT_FILE}" >&2
  exit 1
fi

echo "coverage collection waited for delayed profraw generation"
