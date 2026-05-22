#!/bin/bash
# ============================================================================
# Unified TSDB build script
#
# Usage: ./build.sh --image core|others|dev|core:<version>|others:<version>|dev:<version>
#                   [--arch amd64|arm64|riscv64] [--src PATH] [--cache PATH]
#                   [--clean] [--pull-image] [--split-debug] [--sccache] [component...] [-DKEY=VALUE ...]
#
# --image is required. Use 'core' for engine/taosx/adapter/…; 'others' for
# explorer-ui/insight/connectors.
#
# CORE components (Harbor core image, glibc 2.17):
#   engine, enterprise, adapter, keeper, tools, gen, taosx
#
# OTHERS components (Harbor others image, glibc 2.28):
#   explorer-ui, insight, dotnet, go, jdbc, node, python, rust, odbc
#
# Group shortcuts:
#   core-all   → all CORE components
#   others-all → all OTHERS components
#   all        → all components
#
# -DKEY=VALUE flags are passed directly to cmake after component-generated
# flags, so they can override any component default. Values with spaces must
# be quoted: -D"BUILD_VER_DATE=2026-01-01 00:00:00 +0800"
#
# Examples:
#   ./build.sh --image core engine taosx
#   ./build.sh --image core:3.4.1 --arch arm64 engine adapter
#   ./build.sh --image core engine -DCMAKE_BUILD_TYPE=Debug
#   ./build.sh --image others --pull-image explorer-ui insight jdbc
#   ./build.sh --image core --clean core-all
#   ./build.sh --image core:3.4.1 --arch amd64 \
#       -DBUILD_ENGINE=ON -DCMAKE_BUILD_TYPE=Release \
#       -DBUILD_VER_NUMBER=3.4.1.3 -DBUILD_GITINFO=abc123
# ============================================================================

set -e

# Capture the original invocation before $@ is consumed by argument parsing.
INVOCATION_CMD="$(printf '%q' "$0")$(printf ' %q' "$@")"
declare -a CORE_COMPONENTS=(engine enterprise adapter keeper tools gen taosx)
declare -a OTHERS_COMPONENTS=(explorer-ui insight dotnet go jdbc node python rust odbc)
declare -a ALL_COMPONENTS=("${CORE_COMPONENTS[@]}" "${OTHERS_COMPONENTS[@]}")

# Defaults
# Auto-detect host architecture; can be overridden with --arch
case "$(uname -m)" in
    x86_64)        ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    riscv64)       ARCH="riscv64" ;;
    *)             ARCH="amd64" ;;
esac
IMAGE_OVERRIDE=""
CLEAN=false
PULL_IMAGE=false
SPLIT_DEBUG=false
USE_SCCACHE=false
TSDB_DIR="$(pwd)"
# Cache directory lives outside the source repo so it survives git clean / re-clone.
# Default: $HOME/cache/tsdb-builder. Override via --cache or TSDB_CACHE_DIR env var.
TSDB_CACHE_DIR="${TSDB_CACHE_DIR:-${HOME}/cache/tsdb-builder}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"



declare -a COMPONENTS=()
declare -a EXTRA_CMAKE_ARGS=()
MAKE_TARGET=""

# ============================================================================
# Helper: Print usage
# ============================================================================
usage() {
    cat << 'EOF'
Usage: ./build.sh --image core|others|dev|core:<version>|others:<version>|dev:<version>
                  [--arch amd64|arm64|riscv64] [--src PATH] [--cache PATH]
                  [--clean] [--pull-image] [--split-debug] [--sccache] [component...] [-DKEY=VALUE ...]

--image is required.

CORE components (tsdb-builder-core):
  engine, enterprise, adapter, keeper, tools, gen, taosx

OTHERS components (tsdb-builder-others):
  explorer-ui, insight, dotnet, go, jdbc, node, python, rust, odbc

Groups:
  core-all   (expands to: engine enterprise adapter keeper tools gen taosx)
  others-all (expands to: explorer-ui insight dotnet go jdbc node python rust odbc)
  all        (expands to all components)

Options:
  --image  core|others|dev|core:<version>|others:<version>|dev:<version>  (required)
  --arch   amd64|arm64|riscv64  (default: auto-detected from host)
  --src    /path/to/tsdb        (default: current directory)
  --cache  /path/to/cache       (default: $HOME/cache/tsdb-builder, or TSDB_CACHE_DIR env)
  --clean  wipe build directory before cmake
  --pull-image                  force pull even if the resolved image exists locally
  --split-debug                 separate debug info into .debug/ (objcopy+strip)
  --sccache                     enable sccache for Rust compilation (disables incremental)
  --make-target <target>        make only this target instead of the default all (e.g. build_externals)

cmake passthrough:
  -DKEY=VALUE  passed directly to cmake after component-generated flags;
               may appear multiple times; later values override earlier ones.
EOF
}

# Helper: Check if a value exists in a given array
contains() {
    local value="$1"
    shift
    for item in "$@"; do
        if [[ "$item" == "$value" ]]; then
            return 0
        fi
    done
    return 1
}

# ============================================================================
# Argument parsing
# ============================================================================
while [[ $# -gt 0 ]]; do
    case "$1" in
        --arch)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --arch requires an argument"
                exit 1
            fi
            ARCH="$2"
            shift 2
            ;;
        --cache)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --cache requires an argument"
                exit 1
            fi
            TSDB_CACHE_DIR="$2"
            shift 2
            ;;
        --src)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --src requires an argument"
                exit 1
            fi
            TSDB_DIR="$2"
            shift 2
            ;;
        --image)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --image requires an argument"
                exit 1
            fi
            IMAGE_OVERRIDE="$2"
            shift 2
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        --pull-image)
            PULL_IMAGE=true
            shift
            ;;
        --split-debug)
            SPLIT_DEBUG=true
            shift
            ;;
        --sccache)
            USE_SCCACHE=true
            shift
            ;;
        --make-target)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --make-target requires an argument"
                exit 1
            fi
            MAKE_TARGET="$2"
            shift 2
            ;;
        core-all)
            COMPONENTS+=("${CORE_COMPONENTS[@]}")
            shift
            ;;
        others-all)
            COMPONENTS+=("${OTHERS_COMPONENTS[@]}")
            shift
            ;;
        all)
            COMPONENTS+=("${ALL_COMPONENTS[@]}")
            shift
            ;;
        -D*)
            EXTRA_CMAKE_ARGS+=("$1")
            shift
            ;;
        -*)
            echo "ERROR: Unknown option '$1'"
            usage
            exit 1
            ;;
        *)
            COMPONENTS+=("$1")
            shift
            ;;
    esac
done

# ============================================================================
# Validation
# ============================================================================

# Require at least one component or -D flag
if [[ ${#COMPONENTS[@]} -eq 0 && ${#EXTRA_CMAKE_ARGS[@]} -eq 0 ]]; then
    usage
    exit 1
fi

# --image is required
if [[ -z "$IMAGE_OVERRIDE" ]]; then
    echo "ERROR: --image is required. Use --image core, --image others, or add :<version>."
    exit 1
fi

# Validate ARCH
if [[ "$ARCH" != "amd64" ]] && [[ "$ARCH" != "arm64" ]] && [[ "$ARCH" != "riscv64" ]]; then
    echo "ERROR: Invalid architecture '${ARCH}'. Use 'amd64', 'arm64', or 'riscv64'."
    exit 1
fi

# Validate IMAGE_OVERRIDE
USE_IMAGE="${IMAGE_OVERRIDE%%:*}"
IMAGE_VERSION="latest"
if [[ "$IMAGE_OVERRIDE" == *:* ]]; then
    IMAGE_VERSION="${IMAGE_OVERRIDE#*:}"
fi

if [[ "$USE_IMAGE" != "core" ]] && [[ "$USE_IMAGE" != "others" ]] && [[ "$USE_IMAGE" != "dev" ]]; then
    echo "ERROR: Invalid image '${IMAGE_OVERRIDE}'. Use 'core', 'others', 'dev', or add :<version>."
    exit 1
fi

if [[ -z "$IMAGE_VERSION" ]] || [[ "$IMAGE_VERSION" == *:* ]]; then
    echo "ERROR: Invalid image tag in '${IMAGE_OVERRIDE}'."
    exit 1
fi

# Validate each component exists in ALL_COMPONENTS
for component in "${COMPONENTS[@]}"; do
    if ! contains "$component" "${ALL_COMPONENTS[@]}"; then
        echo "ERROR: Unknown component '$component'"
        exit 1
    fi
done

# ── image selection ───────────────────────────────────────────────────────────
IMAGE="harbor.tdengine.net/tsdb-builder/${USE_IMAGE}:${IMAGE_VERSION}-${ARCH}"
PLATFORM="linux/${ARCH}"

# ── cmake flags ───────────────────────────────────────────────────────────────
# All flags default OFF; only requested components are turned ON.
# Special cases:
#   taosx       → BUILD_TAOSX=ON, BUILD_EXPLORER_UI=OFF (Rust binary only)
#   explorer-ui → BUILD_TAOSX=ON, BUILD_EXPLORER_UI=ON  (Rust + pnpm frontend)

# ── cmake flags ───────────────────────────────────────────────────────────────
# All flags default OFF; only requested components are turned ON.
# Special cases:
#   taosx       → BUILD_TAOSX=ON, BUILD_EXPLORER_UI=OFF (Rust binary only)
#   explorer-ui → BUILD_TAOSX=ON, BUILD_EXPLORER_UI=ON  (Rust + pnpm frontend)
# Uses named variables (flag_BUILD_*) for Bash 3.2 compatibility (no declare -A).

flag_BUILD_ENGINE=OFF     ; flag_BUILD_ENTERPRISE=OFF
flag_BUILD_ADAPTER=OFF    ; flag_BUILD_KEEPER=OFF
flag_BUILD_TOOLS=OFF      ; flag_BUILD_GEN=OFF
flag_BUILD_TAOSX=OFF      ; flag_BUILD_EXPLORER_UI=OFF
flag_BUILD_INSIGHT=OFF    ; flag_BUILD_DOTNET=OFF
flag_BUILD_GO=OFF         ; flag_BUILD_JDBC=OFF
flag_BUILD_NODE=OFF       ; flag_BUILD_PYTHON=OFF
flag_BUILD_RUST=OFF       ; flag_BUILD_ODBC=OFF
flag_BUILD_CONTRIB=OFF

for comp in "${COMPONENTS[@]}"; do
    case "$comp" in
        engine)      flag_BUILD_ENGINE=ON ;;
        enterprise)  flag_BUILD_ENTERPRISE=ON ;;
        adapter)     flag_BUILD_ADAPTER=ON ;;
        keeper)      flag_BUILD_KEEPER=ON ;;
        tools)       flag_BUILD_TOOLS=ON ;;
        gen)         flag_BUILD_GEN=ON ;;
        taosx)       flag_BUILD_TAOSX=ON ;;  # BUILD_EXPLORER_UI stays OFF
        explorer-ui) flag_BUILD_TAOSX=ON; flag_BUILD_EXPLORER_UI=ON ;;
        insight)     flag_BUILD_INSIGHT=ON ;;
        dotnet)      flag_BUILD_DOTNET=ON ;;
        go)          flag_BUILD_GO=ON ;;
        jdbc)        flag_BUILD_JDBC=ON ;;
        node)        flag_BUILD_NODE=ON ;;
        python)      flag_BUILD_PYTHON=ON ;;
        rust)        flag_BUILD_RUST=ON ;;
        odbc)        flag_BUILD_ODBC=ON ;;
    esac
done

# Build ordered cmake arg string (fixed order for reproducibility)
# ${!varname} is indirect expansion, supported since Bash 2.0.
CMAKE_ARGS=""
for flag in BUILD_ENGINE BUILD_ENTERPRISE BUILD_ADAPTER BUILD_KEEPER BUILD_TOOLS BUILD_GEN \
            BUILD_TAOSX BUILD_EXPLORER_UI \
            BUILD_INSIGHT BUILD_DOTNET BUILD_GO BUILD_JDBC BUILD_NODE BUILD_PYTHON \
            BUILD_RUST BUILD_ODBC \
            BUILD_CONTRIB; do
    varname="flag_${flag}"
    CMAKE_ARGS="${CMAKE_ARGS} -D${flag}=${!varname}"
done

# manylinux2014 (core/dev image) workaround: FindThreads tries -lpthreads (non-existent).
# Pre-set all five pthread variables so cmake skips the broken detection and always
# links -lpthread. Without this, targets like taosudf fail to link when
# BUILD_ENTERPRISE=OFF.
if [[ "$USE_IMAGE" == "core" || "$USE_IMAGE" == "dev" ]]; then
    CMAKE_ARGS="${CMAKE_ARGS} \
        -DCMAKE_THREAD_LIBS_INIT=-lpthread \
        -DCMAKE_HAVE_THREADS_LIBRARY=1 \
        -DCMAKE_USE_WIN32_THREADS_INIT=0 \
        -DCMAKE_USE_PTHREADS_INIT=1 \
        -DTHREADS_PREFER_PTHREAD_FLAG=ON"
fi

# riscv64: use mold for C/C++ targets via cmake, but keep GNU ld as system default
# so that Go CGO components (taosadapter, keeper) use GNU ld automatically.
# mold on riscv64 corrupts Go runtime ELF layout (SIGSEGV in pclntab at startup).
if [[ "$ARCH" == "riscv64" ]]; then
    CMAKE_ARGS="${CMAKE_ARGS} -DCMAKE_LINKER=mold"
fi

# GCC 14 (others image) may promote stringop-overflow warnings to errors.
# Some are false positives that would require invasive workarounds.
# Downgrade to warnings so the build succeeds while keeping the check visible.
if [[ "$USE_IMAGE" == "others" ]]; then
    CMAKE_ARGS="${CMAKE_ARGS} -DCMAKE_C_FLAGS=-Wno-error=stringop-overflow -DCMAKE_CXX_FLAGS=-Wno-error=stringop-overflow"
fi


# Sync key flag_BUILD_* with any -D overrides in EXTRA_CMAKE_ARGS (last value wins,
# mirroring cmake's own precedence). Only the flags that drive mount/cleanup decisions
# need tracking here; cmake itself handles the rest.
for _arg in "${EXTRA_CMAKE_ARGS[@]}"; do
    case "$_arg" in
        -DBUILD_ENGINE=ON)       flag_BUILD_ENGINE=ON ;;
        -DBUILD_ENGINE=OFF)      flag_BUILD_ENGINE=OFF ;;
        -DBUILD_TAOSX=ON)        flag_BUILD_TAOSX=ON ;;
        -DBUILD_TAOSX=OFF)       flag_BUILD_TAOSX=OFF ;;
        -DBUILD_EXPLORER_UI=ON)  flag_BUILD_EXPLORER_UI=ON ;;
        -DBUILD_EXPLORER_UI=OFF) flag_BUILD_EXPLORER_UI=OFF ;;
    esac
done

# ── output dir and mounts ─────────────────────────────────────────────────────
if [[ "$USE_IMAGE" == "core" ]]; then
    BUILD_DIR="debug"
elif [[ "$USE_IMAGE" == "dev" ]]; then
    BUILD_DIR="debug-dev"
else
    BUILD_DIR="debug-others"
fi

# Per-image externals isolation: core and others images use different compilers
# (GCC 10 vs GCC 14), so cmake caches compiler paths that are incompatible
# between images. Separate subdirectories prevent cross-image contamination.
EXTERNALS_SUBDIR="externals-${USE_IMAGE}-${ARCH}"
mkdir -p "${TSDB_CACHE_DIR}/${EXTERNALS_SUBDIR}"
declare -a EXTERNALS_MOUNT_ARGS=("--volume=${TSDB_CACHE_DIR}/${EXTERNALS_SUBDIR}:/mnt/.externals")

# CARGO_NET_GIT_FETCH_WITH_CLI: use system git for fetching crates, avoids libgit2
# SSL failures on GitHub-sourced crates (applied to the others image only).
declare -a EXTRA_ENV_ARGS=()
# EXTRA_SECRET_ENV_ARGS: passed to docker run but NOT printed in logs, for sensitive vars.
declare -a EXTRA_SECRET_ENV_ARGS=()
for _salt_var in TD_ENTERPRISE_EDITION_SIGNATURE_SALT; do
    if [[ -n "${!_salt_var:-}" ]]; then
        EXTRA_SECRET_ENV_ARGS+=("--env=${_salt_var}=${!_salt_var}")
    fi
done
declare -a PNPM_STORE_ARGS=()
declare -a JVM_MOUNT_ARGS=()
if [[ "$USE_IMAGE" == "others" ]]; then
    EXTRA_ENV_ARGS+=("--env=CARGO_NET_GIT_FETCH_WITH_CLI=true" "--env=CI=true")
    mkdir -p "${TSDB_CACHE_DIR}/pnpm-store" \
             "${TSDB_CACHE_DIR}/m2-repository" \
             "${TSDB_CACHE_DIR}/nuget"
    PNPM_STORE_ARGS=("--volume=${TSDB_CACHE_DIR}/pnpm-store:/mnt/.pnpm-store")
    JVM_MOUNT_ARGS=(
        "--volume=${TSDB_CACHE_DIR}/m2-repository:/root/.m2/repository"
        "--volume=${TSDB_CACHE_DIR}/nuget:/root/.nuget/packages"
    )
fi

# ccache environment passthrough
EXTRA_ENV_ARGS+=("--env=CCACHE_MAXSIZE=${CCACHE_MAXSIZE:-20G}")
if [[ -n "${CCACHE_REMOTE_STORAGE:-}" ]]; then
    EXTRA_ENV_ARGS+=("--env=CCACHE_REMOTE_STORAGE=${CCACHE_REMOTE_STORAGE}")
fi

# sccache for Rust (opt-in via --sccache)
if [[ "$USE_SCCACHE" == "true" ]]; then
    EXTRA_ENV_ARGS+=("--env=RUSTC_WRAPPER=sccache")
    EXTRA_ENV_ARGS+=("--env=SCCACHE_DIR=/root/.cache/sccache")
    if [[ -n "${SCCACHE_REMOTE_STORAGE:-}" ]]; then
        EXTRA_ENV_ARGS+=("--env=SCCACHE_REMOTE_STORAGE=${SCCACHE_REMOTE_STORAGE}")
    fi
fi

# After a taosx-only (no explorer-ui) build, dist/ exists in the source tree
# (either pre-built or as a placeholder created above). Remove it so a subsequent
# others build can run pnpm normally.
NEEDS_DIST_CLEANUP=false
if [[ "$flag_BUILD_TAOSX" == "ON" && "$flag_BUILD_EXPLORER_UI" == "OFF" ]]; then
    NEEDS_DIST_CLEANUP=true
fi

# Read CONAN_REMOTE_URL from .build-args (same source used by image build scripts).
CONAN_REMOTE_URL=""
if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    CONAN_REMOTE_URL="$(grep -E '^CONAN_REMOTE_URL=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
CONAN_REMOTE_URL="${CONAN_REMOTE_URL:-https://nexus.tdengine.net/repository/conan/}"

# Read GO_PROXY from .build-args and inject as runtime GOPROXY override.
# This ensures the container uses the internal proxy even if the image was built
# with an older default (goproxy.cn).
GO_PROXY=""
if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    GO_PROXY="$(grep -E '^GO_PROXY=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
GO_PROXY="${GO_PROXY:-https://nexus.tdengine.net/repository/goproxy/}"
EXTRA_ENV_ARGS+=("--env=GOPROXY=${GO_PROXY},direct" "--env=GONOSUMCHECK=*" "--env=GONOSUMDB=*")

# Read DEPS_MIRROR_URL from .build-args or use default GitLab generic package registry.
# This is passed as -DLOCAL_URL to cmake so ExternalProject downloads use the internal
# mirror instead of github.com.
DEPS_MIRROR_URL=""
if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    DEPS_MIRROR_URL="$(grep -E '^DEPS_MIRROR_URL=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
DEPS_MIRROR_URL="${DEPS_MIRROR_URL:-https://git.tdengine.net/api/v4/projects/70/packages/generic/externals/latest}"

# Pass the mirror URL to cmake so ExternalProject downloads use the internal
# mirror instead of github.com.
if [[ -n "${DEPS_MIRROR_URL}" ]]; then
    CMAKE_ARGS="${CMAKE_ARGS} -DBUILD_DEPS_MIRROR_URL=${DEPS_MIRROR_URL}"
fi

# Read NPM_REGISTRY_URL from .build-args for container npm/pnpm registry injection.
NPM_REGISTRY_URL=""
if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    NPM_REGISTRY_URL="$(grep -E '^NPM_REGISTRY_URL=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
NPM_REGISTRY_URL="${NPM_REGISTRY_URL:-https://nora.tdengine.net/npm/}"

# Read MAVEN_MIRROR_URL from .build-args for container Maven settings.xml injection.
MAVEN_MIRROR_URL=""
if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    MAVEN_MIRROR_URL="$(grep -E '^MAVEN_MIRROR_URL=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
MAVEN_MIRROR_URL="${MAVEN_MIRROR_URL:-https://nexus.tdengine.net/repository/maven-public/}"

# Read NUGET_SOURCE_URL from .build-args for container NuGet source injection.
NUGET_SOURCE_URL=""
if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    NUGET_SOURCE_URL="$(grep -E '^NUGET_SOURCE_URL=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
NUGET_SOURCE_URL="${NUGET_SOURCE_URL:-https://nora.tdengine.net/nuget/v3/index.json}"

# Read PYPI_INTERNAL_URL from .build-args for container pip index override.
# This is separate from PYPI_MIRROR (used by Dockerfile for image builds).
PYPI_INTERNAL_URL=""
PYPI_INTERNAL_HOST=""
if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    PYPI_INTERNAL_URL="$(grep -E '^PYPI_INTERNAL_URL=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
    PYPI_INTERNAL_HOST="$(grep -E '^PYPI_INTERNAL_HOST=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
PYPI_INTERNAL_URL="${PYPI_INTERNAL_URL:-https://nora.tdengine.net/simple/}"
PYPI_INTERNAL_HOST="${PYPI_INTERNAL_HOST:-nora.tdengine.net}"

TSDB_DIR="$(realpath "${TSDB_DIR}")"

# ── logging ───────────────────────────────────────────────────────────────────
SCRIPT_LOG="${TSDB_DIR}/build.log"
exec > >(tee "$SCRIPT_LOG") 2>&1

BUILD_START=$(date +%s)
_print_timing() {
    local _end _elapsed
    _end=$(date +%s)
    _elapsed=$(( _end - BUILD_START ))
    echo ""
    echo "[INFO] Finished at  : $(date '+%Y-%m-%d %H:%M:%S %Z')"
    printf '[INFO] Total elapsed: %dm%ds\n' $((_elapsed / 60)) $((_elapsed % 60))
}
trap _print_timing EXIT

echo "[INFO] Started at   : $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "[INFO] Log          : ${SCRIPT_LOG}"
echo "[INFO] Command      : ${INVOCATION_CMD}"

# Shell-quote EXTRA_CMAKE_ARGS so that values containing spaces (e.g.
# BUILD_VER_DATE) survive the bash -c string passed to docker run.
EXTRA_CMAKE_ARGS_STR=""
for _arg in "${EXTRA_CMAKE_ARGS[@]}"; do
    EXTRA_CMAKE_ARGS_STR+=" $(printf '%q' "$_arg")"
done

# ── summary ───────────────────────────────────────────────────────────────────
if [[ ${#COMPONENTS[@]} -gt 0 ]]; then
    echo "[INFO] Components : ${COMPONENTS[*]}"
fi
echo "[INFO] Architecture: ${ARCH}"
echo "[INFO] Image tag    : ${IMAGE_VERSION}"
echo "[INFO] Image       : ${IMAGE}"
echo "[INFO] Source      : ${TSDB_DIR}"
echo "[INFO] Cache       : ${TSDB_CACHE_DIR}"
echo "[INFO] Output      : ${TSDB_DIR}/${BUILD_DIR}/"
$CLEAN && echo "[INFO] --clean     : build directory will be wiped before cmake"
$SPLIT_DEBUG && echo "[INFO] --split-debug: debug info will be separated after build"
if [[ ${#EXTRA_CMAKE_ARGS[@]} -gt 0 ]]; then
    echo "[INFO] Extra cmake args (${#EXTRA_CMAKE_ARGS[@]}):"
    for _arg in "${EXTRA_CMAKE_ARGS[@]}"; do
        echo "[INFO]   $_arg"
    done
fi

ensure_image_available() {
    local has_local_image=false

    if docker image inspect "${IMAGE}" >/dev/null 2>&1; then
        has_local_image=true
    fi

    if [[ "${PULL_IMAGE}" == "true" ]]; then
        echo "[INFO] Forcing image pull: ${IMAGE}"
        if ! docker pull "${IMAGE}"; then
            echo "ERROR: Failed to pull ${IMAGE}"
            echo "Run: docker login harbor.tdengine.net"
            exit 1
        fi
        return 0
    fi

    if [[ "${has_local_image}" == "true" ]]; then
        echo "[INFO] Using local image: ${IMAGE}"
        return 0
    fi

    echo "[INFO] Pulling image: ${IMAGE}"
    if ! docker pull "${IMAGE}"; then
        echo "ERROR: Failed to pull ${IMAGE}"
        echo "Run: docker login harbor.tdengine.net"
        exit 1
    fi
}

# ── docker run ────────────────────────────────────────────────────────────────
mkdir -p "${TSDB_CACHE_DIR}/conan2-${ARCH}" \
         "${TSDB_CACHE_DIR}/go-mod" \
         "${TSDB_CACHE_DIR}/go-build" \
         "${TSDB_CACHE_DIR}/cargo-registry" \
         "${TSDB_CACHE_DIR}/cargo-git" \
         "${TSDB_CACHE_DIR}/ccache-${USE_IMAGE}-${ARCH}"
if [[ "$USE_SCCACHE" == "true" ]]; then
    mkdir -p "${TSDB_CACHE_DIR}/sccache-${USE_IMAGE}-${ARCH}"
fi

CONTAINER_SCRIPT="
set -eo pipefail

# ── ccache configuration ─────────────────────────────────────────────────────
# Prepend ccache symlink directory so gcc/g++ calls go through ccache.
# Debian (riscv64) uses /usr/lib/ccache; CentOS/AlmaLinux use /usr/lib64/ccache.
if [ -d /usr/lib64/ccache ]; then
    export PATH=/usr/lib64/ccache:\${PATH}
elif [ -d /usr/lib/ccache ]; then
    export PATH=/usr/lib/ccache:\${PATH}
fi
export CCACHE_MAXSIZE=\${CCACHE_MAXSIZE:-20G}
export CCACHE_BASEDIR=/mnt
export CCACHE_COMPILERCHECK=content
if ! command -v ccache >/dev/null 2>&1; then
    echo '[ERROR] ccache not found in container. Please rebuild or pull the latest Docker image: ${IMAGE}' >&2
    exit 1
fi
ccache --zero-stats >/dev/null 2>&1

# sccache for Rust (opt-in via --sccache)
if [ -n \"\${RUSTC_WRAPPER:-}\" ] && [ \"\${RUSTC_WRAPPER}\" = \"sccache\" ]; then
    if ! command -v sccache >/dev/null 2>&1; then
        echo '[INFO] sccache not found in container — installing prebuilt binary...' >&2
        _sccache_ver=v0.15.0
        _sccache_arch=\$(uname -m)
        _sccache_tar=\"sccache-\${_sccache_ver}-\${_sccache_arch}-unknown-linux-musl\"
        _sccache_url=\"${DEPS_MIRROR_URL}/sccache-\${_sccache_ver}-\${_sccache_arch}-unknown-linux-musl.tar.gz\"
        if curl -fsSL \"\${_sccache_url}\" | tar xz -C /usr/local/bin --strip-components=1 \"\${_sccache_tar}/sccache\" 2>/dev/null; then
            chmod +x /usr/local/bin/sccache
            echo \"[INFO] sccache \${_sccache_ver} installed successfully\"
        else
            echo '[WARN] sccache prebuilt download failed, disabling RUSTC_WRAPPER' >&2
            unset RUSTC_WRAPPER
        fi
        unset _sccache_ver _sccache_arch _sccache_tar _sccache_url
    fi
    if command -v sccache >/dev/null 2>&1; then
        sccache --zero-stats >/dev/null 2>&1 || true
        echo \"[INFO] sccache enabled (cache dir: \${SCCACHE_DIR:-/root/.cache/sccache})\"
    fi
fi

if [ '${CLEAN}' = 'true' ]; then
    rm -rf /mnt/${BUILD_DIR}
fi
mkdir -p /mnt/${BUILD_DIR}
cd /mnt/${BUILD_DIR}

if [ ! -f /root/.conan2/profiles/default ]; then
    conan profile detect --force
    sed -i 's/compiler.cppstd=gnu14/compiler.cppstd=gnu17/' /root/.conan2/profiles/default
fi
# Ensure the arch in the conan profile matches the actual container arch.
# conan profile detect may record x86_64 even on aarch64 if it mis-detects.
_host_arch=\$(uname -m)
case "\$_host_arch" in
    aarch64|arm64) _conan_arch=armv8 ;;
    x86_64)        _conan_arch=x86_64 ;;
    *)             _conan_arch=\$_host_arch ;;
esac
sed -i "s/^arch=.*/arch=\$_conan_arch/" /root/.conan2/profiles/default

# Declare system-installed build tools so Conan does not download them from remotes.
if ! grep -q platform_tool_requires /root/.conan2/profiles/default 2>/dev/null; then
    cat >> /root/.conan2/profiles/default << PROFILE_EOF

[platform_tool_requires]
cmake/3.21.5
automake/1.16.5
autoconf/2.71
m4/1.4.19
PROFILE_EOF
fi

# Restore nexus remote: the volume mount on /root/.conan2 shadows the
# image-baked Conan config, so the nexus remote added during image build
# is lost.  Re-add it at index 0 (highest priority) if missing.
# Skip if the remote URL is unreachable (e.g. decommissioned).
if [ -n '${CONAN_REMOTE_URL}' ] && ! conan remote list | grep -q nexus; then
    if curl -sfI '${CONAN_REMOTE_URL}' >/dev/null 2>&1; then
        conan remote add nexus '${CONAN_REMOTE_URL}' --index 0
    else
        echo '[WARN] Nexus remote unreachable, skipping.'
    fi
fi

# Configure npm/pnpm registry → internal mirror (others image only)
if command -v npm >/dev/null 2>&1; then
    npm config set registry '${NPM_REGISTRY_URL}' 2>/dev/null || true
    echo \"[INFO] npm registry set to '${NPM_REGISTRY_URL}'\"
fi

# Configure Maven mirror → internal Nexus (others image only)
if command -v mvn >/dev/null 2>&1 && [ ! -f /root/.m2/settings.xml ]; then
    mkdir -p /root/.m2
    cat > /root/.m2/settings.xml << MAVEN_SETTINGS_EOF
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<settings>
  <mirrors>
    <mirror>
      <id>nexus-internal</id>
      <mirrorOf>*</mirrorOf>
      <url>${MAVEN_MIRROR_URL}</url>
    </mirror>
  </mirrors>
</settings>
MAVEN_SETTINGS_EOF
    echo '[INFO] Maven settings.xml written with internal Nexus mirror'
fi

# Configure NuGet source → internal mirror (others image only)
if command -v dotnet >/dev/null 2>&1; then
    dotnet nuget add source '${NUGET_SOURCE_URL}' --name tdengine-internal 2>/dev/null || true
    echo '[INFO] NuGet source added: ${NUGET_SOURCE_URL}'
fi

# Override pip index-url → internal mirror (image bakes Aliyun; runtime overrides to Nora)
if command -v pip3 >/dev/null 2>&1; then
    pip3 config set global.index-url '${PYPI_INTERNAL_URL}' 2>/dev/null || true
    pip3 config set global.trusted-host '${PYPI_INTERNAL_HOST}' 2>/dev/null || true
    echo \"[INFO] pip index-url overridden to '${PYPI_INTERNAL_URL}'\"
fi

# libcrypt (libxcrypt) is installed under /usr/local/lib in the manylinux
# container. Neither gcc nor mold search /usr/local/lib by default, so
# cyrus-sasl (pulled by librdkafka with sasl=True) fails to find -lcrypt.
# LIBRARY_PATH makes gcc pass -L/usr/local/lib to the linker (mold).
export LIBRARY_PATH="/usr/local/lib\${LIBRARY_PATH:+:\$LIBRARY_PATH}"

# Sanitizer builds (-DBUILD_SANITIZER=ON): mold cannot find libasan_preinit.o,
# libubsan.so, etc. because devtoolset-7 only ships 32-bit sanitizer libs.
# Add the devtoolset-10 gcc lib directory (which has 64-bit asan+ubsan) to
# LIBRARY_PATH so gcc passes -L to mold and all sanitizer runtimes resolve.
_san_dir=/opt/rh/devtoolset-10/root/usr/lib/gcc/x86_64-redhat-linux/10
if [ -d "\$_san_dir" ]; then
    export LIBRARY_PATH="\${_san_dir}:\${LIBRARY_PATH}"
fi

# Remove stale host-generated .env and dev db so build.rs recreates both inside
# the container with the correct /mnt/... path and a fresh schema.
rm -f /mnt/source/taos-xservice/.env \
      /mnt/source/taos-xservice/target/taosx.dev.db

# When building taosx without explorer-ui (core image), cmake requires
# dist/index.html to pre-exist because pnpm is not available.  Create a
# placeholder so cmake configure succeeds; NEEDS_DIST_CLEANUP removes it
# after the build so a subsequent explorer-ui build can run pnpm normally.
if [ '${flag_BUILD_TAOSX}' = 'ON' ] && [ '${flag_BUILD_EXPLORER_UI}' = 'OFF' ]; then
    if [ ! -f /mnt/source/taos-xservice/explorer/dist/index.html ]; then
        mkdir -p /mnt/source/taos-xservice/explorer/dist
        echo '<!-- placeholder for taosx-only build -->' > /mnt/source/taos-xservice/explorer/dist/index.html
    fi
fi

cmake .. ${CMAKE_ARGS}${EXTRA_CMAKE_ARGS_STR}

MAKEFLAGS= make -j \$(nproc) ${MAKE_TARGET} || {
    echo '[WARN] Parallel build failed (ExternalProject jobserver issue on make 3.82); retrying with -j1...'
    MAKEFLAGS= make -j1 ${MAKE_TARGET}
}

# ── split debug info (--split-debug) ──────────────────────────────────────────
# Separates DWARF debug info from C/Go binaries and shared libraries into
# .debug/ subdirectories, then strips the originals. GDB auto-discovers
# .debug/<name>.debug via the .gnu_debuglink section.
#
# Executables: strip -s (remove all symbols — maximum size reduction)
# Shared libs: strip --strip-debug (keep dynamic symbols required for linking)
if [ '${SPLIT_DEBUG}' = 'true' ]; then
    _split_count=0

    # --- executables in build/bin/ ---
    _bin_dir=/mnt/${BUILD_DIR}/build/bin
    _debug_dir=\${_bin_dir}/.debug
    mkdir -p \"\${_debug_dir}\"
    for _binary in taosd taos taosql taosmqtt taosudf taosgen taosadapter taoskeeper; do
        _path=\"\${_bin_dir}/\${_binary}\"
        [ -f \"\${_path}\" ] || continue
        if ! file \"\${_path}\" | grep -q ELF; then
            echo '[WARN] Skipping non-ELF file: '\"\${_binary}\"
            continue
        fi
        echo '[INFO] Splitting debug info: bin/'\"\${_binary}\"
        objcopy --only-keep-debug \"\${_path}\" \"\${_debug_dir}/\${_binary}.debug\"
        strip -s \"\${_path}\"
        objcopy --add-gnu-debuglink=\"\${_debug_dir}/\${_binary}.debug\" \"\${_path}\"
        _split_count=\$((_split_count + 1))
    done

    # --- shared libraries in build/lib/ ---
    _lib_dir=/mnt/${BUILD_DIR}/build/lib
    _lib_debug_dir=\${_lib_dir}/.debug
    mkdir -p \"\${_lib_debug_dir}\"
    for _sofile in libtaos.so libtaosnative.so; do
        _path=\"\${_lib_dir}/\${_sofile}\"
        [ -f \"\${_path}\" ] || continue
        echo '[INFO] Splitting debug info: lib/'\"\${_sofile}\"
        objcopy --only-keep-debug \"\${_path}\" \"\${_lib_debug_dir}/\${_sofile}.debug\"
        strip --strip-debug \"\${_path}\"
        objcopy --add-gnu-debuglink=\"\${_lib_debug_dir}/\${_sofile}.debug\" \"\${_path}\"
        _split_count=\$((_split_count + 1))
    done

    echo '[INFO] Debug info separated for '\"\${_split_count}\"' files'
    echo '[INFO]   bin → '\"\${_debug_dir}/\"
    echo '[INFO]   lib → '\"\${_lib_debug_dir}/\"
fi

echo ''
echo '── ccache statistics ──────────────────────────────────────────────────────'
ccache --show-stats

if [ -n \"\${RUSTC_WRAPPER:-}\" ] && [ \"\${RUSTC_WRAPPER}\" = \"sccache\" ] && command -v sccache >/dev/null 2>&1; then
    echo ''
    echo '── sccache statistics ─────────────────────────────────────────────────────'
    sccache --show-stats
fi

if [ '${NEEDS_DIST_CLEANUP}' = 'true' ]; then
    rm -rf /mnt/source/taos-xservice/explorer/dist
fi
"

declare -a DOCKER_MAIN_ARGS=(
    "--rm"
    $( [[ "$ARCH" != "riscv64" ]] && echo "--platform=${PLATFORM}" )
    "${EXTRA_ENV_ARGS[@]}"
    "${EXTERNALS_MOUNT_ARGS[@]}"
    "${PNPM_STORE_ARGS[@]}"
    "${JVM_MOUNT_ARGS[@]}"
    "--volume=${TSDB_DIR}:/mnt"
    "--volume=${TSDB_CACHE_DIR}/conan2-${ARCH}:/root/.conan2"
    "--volume=${TSDB_CACHE_DIR}/go-mod:/root/go/pkg/mod"
    "--volume=${TSDB_CACHE_DIR}/go-build:/root/.cache/go-build"
    "--volume=${TSDB_CACHE_DIR}/cargo-registry:/root/.cargo/registry"
    "--volume=${TSDB_CACHE_DIR}/cargo-git:/root/.cargo/git"
    "--volume=${TSDB_CACHE_DIR}/ccache-${USE_IMAGE}-${ARCH}:/root/.cache/ccache"
    "--volume=${SCRIPT_DIR}/.cargo/config.toml:/root/.cargo/config.toml:ro"
)
if [[ "$USE_SCCACHE" == "true" ]]; then
    DOCKER_MAIN_ARGS+=("--volume=${TSDB_CACHE_DIR}/sccache-${USE_IMAGE}-${ARCH}:/root/.cache/sccache")
fi

echo "[INFO] docker run command:"
printf '  docker run'
for _arg in "${DOCKER_MAIN_ARGS[@]}"; do
    printf ' \\\n    %s' "$(printf '%q' "$_arg")"
done
printf " \\
    %s" "$(printf "%q" "${IMAGE}")"
printf " \\
    bash -c %s\n" "$(printf "%q" "$CONTAINER_SCRIPT")"
echo ""

ensure_image_available
docker run "${DOCKER_MAIN_ARGS[@]}" "${EXTRA_SECRET_ENV_ARGS[@]}" "${IMAGE}" bash -c "$CONTAINER_SCRIPT"

# ODBC tests are configured in container path (/mnt/...), which makes host-side
# ctest resolve to an empty test set. Rewrite generated CTest metadata to host paths.
if [[ "$flag_BUILD_ODBC" == "ON" ]]; then
    ODBC_TEST_META_DIR="${TSDB_DIR}/${BUILD_DIR}/build/taos-connector-odbc/build"
    if [[ -d "${ODBC_TEST_META_DIR}" ]]; then
        while IFS= read -r _meta; do
            sed -i "s#/mnt#${TSDB_DIR}#g" "${_meta}"
        done < <(find "${ODBC_TEST_META_DIR}" -type f \( -name "DartConfiguration.tcl" -o -name "CTestTestfile.cmake" \))
        echo "[INFO] Rewrote ODBC CTest metadata paths for host ctest: ${ODBC_TEST_META_DIR}"
    fi
fi 

echo "[INFO] Build completed. Artifacts: ${TSDB_DIR}/${BUILD_DIR}/"
if $SPLIT_DEBUG; then
    echo "[INFO] Debug symbols: ${TSDB_DIR}/${BUILD_DIR}/build/bin/.debug/"
    echo "[INFO]                ${TSDB_DIR}/${BUILD_DIR}/build/lib/.debug/"
fi
