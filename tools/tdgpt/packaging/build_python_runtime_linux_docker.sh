#!/usr/bin/env bash

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python_version="3.12.16"
work_dir="${PWD}/tdgpt-python-runtime-docker-build"
output_dir="${PWD}/dist"
arch=""
image="tdgpt-python-runtime:manylinux2014"
# Pin the image toolchain; the invoked builder separately enforces its minimum.
pinned_uv_version="0.13.9"

usage() {
  cat <<'EOF'
Usage: build_python_runtime_linux_docker.sh [options]

Build the portable CPython runtime with uv inside a manylinux2014 Docker image.
The command must run on native x86_64 or aarch64 hardware.

Options:
  --python-version VERSION  Exact CPython runtime version. Default: 3.12.16
  --work-dir DIR            Host workspace. Default: ./tdgpt-python-runtime-docker-build
  --output-dir DIR          Host output directory. Default: ./dist
  --arch x64|arm64          Native architecture; inferred when omitted.
  --image IMAGE             Image reference prefix (repository[:tag], registry ports allowed);
                            Python version and architecture are appended as a tag suffix.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python-version|--work-dir|--output-dir|--arch|--image)
      [[ $# -ge 2 && -n "$2" && "$2" != --* ]] || {
        echo "$1 requires a value" >&2
        exit 2
      }
      case "$1" in
        --python-version) python_version="$2" ;;
        --work-dir) work_dir="$2" ;;
        --output-dir) output_dir="$2" ;;
        --arch) arch="$2" ;;
        --image) image="$2" ;;
      esac
      shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$(uname -s)" == "Linux" ]] || { echo "This builder must run on Linux" >&2; exit 1; }
[[ -f "$script_dir/Dockerfile.python-runtime" ]] || {
  echo "Missing Dockerfile.python-runtime under: $script_dir" >&2
  exit 1
}
case "$(uname -m)" in
  x86_64) native_arch=x64; docker_arch=amd64; base_image=quay.io/pypa/manylinux2014_x86_64 ;;
  aarch64|arm64) native_arch=arm64; docker_arch=arm64; base_image=quay.io/pypa/manylinux2014_aarch64 ;;
  *) echo "Unsupported Linux architecture: $(uname -m)" >&2; exit 1 ;;
esac
arch="${arch:-$native_arch}"
case "$arch" in
  x64|arm64) ;;
  *) echo "--arch must be x64 or arm64, got: $arch" >&2; exit 2 ;;
esac
command -v docker >/dev/null 2>&1 || { echo "docker is required" >&2; exit 1; }
[[ "$arch" == "$native_arch" ]] || {
  echo "--arch $arch must be built on native $native_arch hardware" >&2
  exit 1
}

mkdir -p "$work_dir" "$output_dir"
work_dir="$(cd "$work_dir" && pwd)"
output_dir="$(cd "$output_dir" && pwd)"
mkdir -p "$work_dir/home" "$work_dir/uv-cache"
[[ -w "$work_dir" && -w "$work_dir/home" && -w "$work_dir/uv-cache" && -w "$output_dir" ]] || {
  echo "Docker runtime build directories must be writable: $work_dir and $output_dir" >&2
  exit 1
}
if [[ "${image##*/}" == *:* ]]; then
  image_tag="${image}-${python_version}-${docker_arch}"
else
  image_tag="${image}:${python_version}-${docker_arch}"
fi
docker build --pull --platform="linux/${docker_arch}" \
  --build-arg "BASE_IMAGE=${base_image}" --build-arg "UV_VERSION=${pinned_uv_version}" \
  -f "$script_dir/Dockerfile.python-runtime" -t "$image_tag" "$script_dir"
docker run --rm --platform="linux/${docker_arch}" --env "EXPECTED_UV_VERSION=${pinned_uv_version}" --entrypoint /bin/sh "$image_tag" -c \
  'missing=0;
   command -v uv >/dev/null 2>&1 || { echo "missing: uv" >&2; missing=1; };
   uv_version="$(uv --version 2>/dev/null)";
   uv_version="${uv_version#uv }";
   uv_version="${uv_version%% *}";
   [ "$uv_version" = "$EXPECTED_UV_VERSION" ] || { echo "expected pinned uv $EXPECTED_UV_VERSION; found ${uv_version:-unknown}" >&2; missing=1; };
   for tool in xz find; do command -v "$tool" >/dev/null 2>&1 || { echo "missing: $tool" >&2; missing=1; }; done;
   command -v patchelf >/dev/null 2>&1 || { echo "missing: patchelf" >&2; missing=1; };
   command -v ldconfig >/dev/null 2>&1 && ldconfig -p 2>/dev/null | grep -q "libsqlite3.so" || { echo "missing: sqlite3 runtime library" >&2; missing=1; };
   exit "$missing"'
# Probe using the actual container UID and mounts: host -w cannot detect userns
# remapping or a read-only bind mount. Fail before downloading a Python runtime.
docker run --rm --platform="linux/${docker_arch}" \
  --user "$(id -u):$(id -g)" \
  -v "${work_dir}:/work" -v "${output_dir}:/out" \
  --entrypoint /bin/sh "$image_tag" -c \
  'for dir in /work /work/home /work/uv-cache /out; do
     probe=$(mktemp "$dir/.tdgpt-write-probe.XXXXXX") && rm -f "$probe" || {
       echo "Container cannot write $dir; check bind-mount permissions and Docker user namespace mapping" >&2; exit 1;
     };
   done'
docker run --rm --platform="linux/${docker_arch}" \
  --user "$(id -u):$(id -g)" \
  --env HOME=/work/home --env UV_CACHE_DIR=/work/uv-cache \
  -v "${work_dir}:/work" \
  -v "${output_dir}:/out" \
  "$image_tag" \
  --python-version "$python_version" \
  --arch "$arch" \
  --work-dir /work \
  --output-dir /out
