#!/usr/bin/env bash
# Build all Linux TDgpt offline deliverables on their native architecture.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
tdgpt_root=$(cd "${script_dir}/.." && pwd)
version=""
edition="enterprise"
tdmodel_requirements=""
work_dir="${PWD}/tdgpt-linux-offline-release-build"
output_dir="${PWD}/dist"
arch=""
python_version="3.12.12"
target_python_home="/usr/local/taos-deps/python-runtime"
pip_extra_args=""
lite=0
venv_version="1.0.1"

usage() {
  cat <<'EOF'
Usage: build_linux_offline_release.sh --version VERSION --tdmodel-requirements FILE [options]

Builds the CPython runtime, TDgpt and TDModel venv archives, and the TDgpt
installer. All artifacts and SHA256SUMS are written to --output-dir.

Required:
  --version VERSION              TDgpt package version passed to release.sh.
  --tdmodel-requirements FILE    TDModel requirements.txt.

Options:
  --edition enterprise|community Installer edition. Default: enterprise
  --tdgpt-root DIR               TDgpt source root. Default: inferred from this script
  --work-dir DIR                 Temporary build workspace.
  --output-dir DIR               Deliverable directory. Default: ./dist
  --arch x64|arm64               Validate native architecture and name outputs.
  --python-version VERSION       CPython version. Default: 3.12.12
  --target-python-home DIR       Runtime path written into venvs.
                                 Default: /usr/local/taos-deps/python-runtime
  --pip-extra-args STRING        Extra pip arguments passed to venv builder.
  --lite                         Build Lite installer and Lite TDgpt venv archives.
  --venv-version VERSION         Version used in venv archive names. Default: 1.0.1.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) version="$2"; shift 2 ;;
    --edition) edition="$2"; shift 2 ;;
    --tdmodel-requirements) tdmodel_requirements="$2"; shift 2 ;;
    --tdgpt-root) tdgpt_root="$2"; shift 2 ;;
    --work-dir) work_dir="$2"; shift 2 ;;
    --output-dir) output_dir="$2"; shift 2 ;;
    --arch) arch="$2"; shift 2 ;;
    --python-version) python_version="$2"; shift 2 ;;
    --target-python-home) target_python_home="$2"; shift 2 ;;
    --pip-extra-args) pip_extra_args="$2"; shift 2 ;;
    --lite) lite=1; shift ;;
    --venv-version) venv_version="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$version" ]] || { echo "--version is required" >&2; exit 2; }
[[ -n "$tdmodel_requirements" ]] || { echo "--tdmodel-requirements is required" >&2; exit 2; }
case "$edition" in enterprise|community) ;; *) echo "Unsupported edition: $edition" >&2; exit 2 ;; esac
tdgpt_root=$(cd "$tdgpt_root" && pwd)
tdmodel_requirements=$(cd "$(dirname "$tdmodel_requirements")" && pwd)/$(basename "$tdmodel_requirements")
mkdir -p "$work_dir" "$output_dir"

runtime_args=(
  --python-version "$python_version" \
  --work-dir "${work_dir}/runtime" \
  --output-dir "$output_dir"
)
if [[ -n "$arch" ]]; then
  runtime_args+=(--arch "$arch")
fi
"${script_dir}/build_python_runtime_linux.sh" "${runtime_args[@]}"

runtime_home="${work_dir}/runtime/python-runtime-${python_version}"
venv_args=(
  --tdgpt-root "$tdgpt_root" \
  --build-python-home "$runtime_home" \
  --target-python-home "$target_python_home" \
  --tdmodel-requirements "$tdmodel_requirements" \
  --work-dir "${work_dir}/venvs" \
  --output-dir "$output_dir"
)
if [[ -n "$arch" ]]; then
  venv_args+=(--arch "$arch")
fi
if [[ -n "$pip_extra_args" ]]; then
  venv_args+=(--pip-extra-args "$pip_extra_args")
fi
if [[ "$lite" == "1" ]]; then
  venv_args+=(--lite)
fi
venv_args+=(--venv-version "$venv_version")
"${script_dir}/build_linux_offline_assets.sh" "${venv_args[@]}"

pushd "${tdgpt_root}/script" >/dev/null
release_args=(-e "$edition" -v "$version")
if [[ "$lite" == "1" ]]; then
  release_args+=(-l)
fi
bash release.sh "${release_args[@]}"
popd >/dev/null

case "$(uname -m)" in
  x86_64) native_arch=x64 ;;
  aarch64|arm64) native_arch=arm64 ;;
  *) echo "Unsupported Linux architecture: $(uname -m)" >&2; exit 1 ;;
esac
arch="${arch:-$native_arch}"
product_name="tdengine-tdgpt-enterprise"
[[ "$edition" == "community" ]] && product_name="tdengine-tdgpt-oss"
offline_asset_prefix="tdengine-tdgpt"
if [[ "$lite" == "1" ]]; then
  product_name="${product_name}-lite"
  offline_asset_prefix="tdengine-tdgpt-lite"
fi
installer="${tdgpt_root}/release/${product_name}-${version}-linux-${arch}.tar.gz"
[[ -f "$installer" ]] || { echo "Installer was not produced: $installer" >&2; exit 1; }
cp -f "$installer" "$output_dir/"

artifacts=(
  "${output_dir}/tdengine-python-runtime-${python_version}-linux-${arch}.tar.gz"
  "${output_dir}/${offline_asset_prefix}-venv-${venv_version}-linux-${arch}.tar.gz"
  "${output_dir}/${offline_asset_prefix}-model-venv-${venv_version}-linux-${arch}.tar.gz"
  "${output_dir}/tdengine-tdmodel-venv-${venv_version}-linux-${arch}.tar.gz"
  "${output_dir}/$(basename "$installer")"
)
for artifact in "${artifacts[@]}"; do
  [[ -f "$artifact" ]] || { echo "Missing artifact: $artifact" >&2; exit 1; }
done
(
  cd "$output_dir"
  sha256sum "${artifacts[@]##*/}" > SHA256SUMS
)
cat "${output_dir}/SHA256SUMS"
