#!/bin/bash
# ============================================================================
# prepare-externals.sh — Download, rename, hash, and upload ExternalProject
# source tarballs to GitLab Generic Package Registry.
#
# The dependency list is auto-extracted from external.cmake by parsing all
# get_from_local_if_exists() calls. No hardcoded DEPS array to maintain.
#
# Usage:
#   export GITLAB_TOKEN="glpat-xxxx"
#   export GITLAB_PROJECT_ID="123"
#
#   # Bulk: download + hash all deps (dry run)
#   ./scripts/prepare-externals.sh --cmake path/to/external.cmake
#
#   # Bulk: download + hash + upload all deps
#   ./scripts/prepare-externals.sh --cmake path/to/external.cmake --upload
#
#   # Single dep: download + hash + upload one dep by mirror filename
#   ./scripts/prepare-externals.sh --cmake path/to/external.cmake --upload zlib-v1.3.1.tar.gz
#
#   # Add a new dep (not yet in external.cmake)
#   ./scripts/prepare-externals.sh --upload --add "newlib-v1.0.tar.gz|https://..."
#
#   # Verify: check that all manifest entries exist on registry
#   ./scripts/prepare-externals.sh --verify
#
#   # Verify: check current external.cmake deps directly
#   ./scripts/prepare-externals.sh --cmake path/to/external.cmake --verify
#
#   # List: show all deps extracted from external.cmake
#   ./scripts/prepare-externals.sh --cmake path/to/external.cmake --list
# ============================================================================
set -euo pipefail

# ── sha256 portability (Linux: sha256sum, macOS: shasum -a 256) ──────────
if command -v sha256sum &>/dev/null; then
    sha256_cmd() { sha256sum "$1" | cut -d' ' -f1; }
else
    sha256_cmd() { shasum -a 256 "$1" | cut -d' ' -f1; }
fi

GITLAB_URL="${GITLAB_URL:-https://git.tdengine.net}"
PACKAGE_NAME="externals"
PACKAGE_VERSION="latest"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="${SCRIPT_DIR}/externals-manifest.txt"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS] [FILENAME]

Options:
  --cmake FILE         Path to external.cmake (auto-extract deps from cmake)
  --upload             Upload tarballs to GitLab Package Registry
  --verify             Check registry entries from manifest or --cmake deps
  --add "name|url"     Add a dep not yet in external.cmake (requires --upload)
  --list               List all deps extracted from external.cmake and exit
  -h, --help           Show this help

Arguments:
  FILENAME             Process only this dep (by mirror filename)

Environment:
  GITLAB_TOKEN         GitLab PAT with api scope (required for --upload/--verify)
  GITLAB_PROJECT_ID    Target project ID (required for --upload/--verify)

Examples:
  $(basename "$0") --cmake source/taos-community/cmake/external.cmake
  $(basename "$0") --cmake source/taos-community/cmake/external.cmake --upload
  $(basename "$0") --cmake source/taos-community/cmake/external.cmake --upload zlib-v1.3.1.tar.gz
  $(basename "$0") --cmake source/taos-community/cmake/external.cmake --verify
  $(basename "$0") --cmake source/taos-community/cmake/external.cmake --list
  $(basename "$0") --upload --add "foo-v1.tar.gz|https://example.com/foo.tar.gz"
  $(basename "$0") --verify
EOF
}

# ── Parse arguments ──────────────────────────────────────────────────────────
DO_UPLOAD=false
DO_VERIFY=false
DO_LIST=false
ADD_ENTRY=""
FILTER_NAME=""
CMAKE_FILE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cmake)
            if [[ $# -lt 2 ]]; then echo "ERROR: --cmake requires a file path"; exit 1; fi
            CMAKE_FILE="$2"; shift 2
            ;;
        --upload) DO_UPLOAD=true; shift ;;
        --verify) DO_VERIFY=true; shift ;;
        --list)   DO_LIST=true; shift ;;
        --add)
            if [[ $# -lt 2 ]]; then echo "ERROR: --add requires an argument"; exit 1; fi
            ADD_ENTRY="$2"; shift 2
            ;;
        -h|--help) usage; exit 0 ;;
        -*)       echo "ERROR: Unknown option: $1"; usage; exit 1 ;;
        *)        FILTER_NAME="$1"; shift ;;
    esac
done

# ── Validate argument combinations ───────────────────────────────────────────
if $DO_VERIFY && $DO_UPLOAD; then
    echo "ERROR: --verify and --upload are mutually exclusive"
    exit 1
fi

if [[ -n "$ADD_ENTRY" ]] && ! $DO_UPLOAD; then
    echo "ERROR: --add requires --upload"
    exit 1
fi

if $DO_UPLOAD && [[ -z "${GITLAB_TOKEN:-}" ]]; then
    echo "ERROR: GITLAB_TOKEN must be set for upload"
    exit 1
fi

if $DO_VERIFY && [[ -z "${GITLAB_TOKEN:-}" ]]; then
    echo "ERROR: GITLAB_TOKEN must be set for verify"
    exit 1
fi

if ($DO_UPLOAD || $DO_VERIFY) && [[ -z "${GITLAB_PROJECT_ID:-}" ]]; then
    echo "ERROR: GITLAB_PROJECT_ID must be set"
    exit 1
fi
PROJECT_ID="${GITLAB_PROJECT_ID:-}"

# ── Extract DEPS from external.cmake ────────────────────────────────────────
# Parses all get_from_local_if_exists() calls:
#   Two-arg calls → mirror_filename|upstream_url
#   One-arg calls → filename extracted from URL's last path segment
#
# CPython SDK uses cmake variables (_pyudf_pbs_release, _pyver, _pbs_triple)
# instead of literal URLs.  We resolve those by extracting the version and
# release tag from the cmake file, then expanding all platform triples.
extract_deps_from_cmake() {
    local cmake_file="$1"
    # ── Literal get_from_local_if_exists() calls ──
    perl -0777 -ne '
        # Two-arg calls: get_from_local_if_exists("url" "mirror_filename")
        while (/get_from_local_if_exists\(\s*"([^"]+\.tar\.gz)"\s+"([^"]+\.tar\.gz)"\s*\)/g) {
            print "$2|$1\n";
        }
        # One-arg calls: get_from_local_if_exists("url")
        while (/get_from_local_if_exists\(\s*"([^"]+\.tar\.gz)"\s*\)/g) {
            my $url = $1;
            my $name = $url;
            $name =~ s|.*/||;  # extract last path segment
            print "$name|$url\n";
        }
    ' "$cmake_file"

    # ── CPython SDK: expand platform triples ──
    # Extract _pyudf_pbs_release and BUILD_PYUDF_PYTHON_VERSION default
    local pbs_release pyver
    pbs_release=$(sed -n 's/^.*set(_pyudf_pbs_release  *"\([^"]*\)").*/\1/p' "$cmake_file" | head -1)
    # Extract version from the FATAL_ERROR hint: STRING=\"3.15.0b1\"
    pyver=$(sed -n 's/.*BUILD_PYUDF_PYTHON_VERSION:STRING=\\"\([0-9][0-9a-z.]*\)\\.*/\1/p' "$cmake_file" | head -1)

    if [[ -n "$pbs_release" && -n "$pyver" ]]; then
        local triples=(
            "aarch64-unknown-linux-gnu"
            "x86_64-unknown-linux-gnu"
            "aarch64-apple-darwin"
            "x86_64-apple-darwin"
            "aarch64-pc-windows-msvc"
            "x86_64-pc-windows-msvc"
        )
        local base_url="https://github.com/astral-sh/python-build-standalone/releases/download/${pbs_release}"
        for triple in "${triples[@]}"; do
            local archive="cpython-${pyver}+${pbs_release}-${triple}-install_only.tar.gz"
            echo "${archive}|${base_url}/${archive}"
        done
    fi
}

declare -a DEPS=()

if [[ -n "$CMAKE_FILE" ]]; then
    if [[ ! -f "$CMAKE_FILE" ]]; then
        echo "ERROR: cmake file not found: ${CMAKE_FILE}"
        exit 1
    fi
    echo "[INFO] Extracting deps from: ${CMAKE_FILE}"
    while IFS= read -r line; do
        [[ -n "$line" ]] && DEPS+=("$line")
    done < <(extract_deps_from_cmake "$CMAKE_FILE" | sort -u)
    echo "[INFO] Found ${#DEPS[@]} deps"
    echo ""
fi

# ── --add: append ephemeral entry ────────────────────────────────────────────
if [[ -n "$ADD_ENTRY" ]]; then
    add_name="${ADD_ENTRY%%|*}"
    add_url="${ADD_ENTRY##*|}"
    if [[ -z "$add_name" || -z "$add_url" || "$add_name" == "$add_url" ]]; then
        echo "ERROR: --add format must be 'filename|url'"
        exit 1
    fi
    DEPS+=("$ADD_ENTRY")
    echo "[INFO] Added ephemeral dep: ${add_name}"
    echo "  NOTE: To make permanent, add get_from_local_if_exists() call in external.cmake"
    echo ""
fi

# ── --list: print extracted deps and exit ────────────────────────────────────
if $DO_LIST; then
    if [[ ${#DEPS[@]} -eq 0 ]]; then
        echo "ERROR: --list requires --cmake"
        exit 1
    fi
    printf "%-50s %s\n" "MIRROR FILENAME" "UPSTREAM URL"
    printf "%-50s %s\n" "──────────────" "────────────"
    for entry in "${DEPS[@]}"; do
        name="${entry%%|*}"
        url="${entry##*|}"
        printf "%-50s %s\n" "$name" "$url"
    done
    echo ""
    echo "Total: ${#DEPS[@]} deps"
    exit 0
fi

# ── Ensure we have deps to process ───────────────────────────────────────────
if [[ ${#DEPS[@]} -eq 0 ]] && ! $DO_VERIFY; then
    echo "ERROR: No deps to process. Use --cmake to extract from external.cmake, or --add to specify manually."
    exit 1
fi

# ── Registry URL helper ─────────────────────────────────────────────────────
registry_url() {
    local filename="$1"
    echo "${GITLAB_URL}/api/v4/projects/${PROJECT_ID}/packages/generic/${PACKAGE_NAME}/${PACKAGE_VERSION}/${filename}"
}

warn_if_manifest_stale() {
    local manifest_names deps_names

    [[ -n "$CMAKE_FILE" ]] || return 0
    [[ ${#DEPS[@]} -gt 0 ]] || return 0
    [[ -z "$FILTER_NAME" ]] || return 0
    [[ -f "$MANIFEST" ]] || return 0

    manifest_names="$(mktemp)"
    deps_names="$(mktemp)"

    awk '!/^#/ && NF >= 2 { print $2 }' "$MANIFEST" | sort -u > "$manifest_names"
    for entry in "${DEPS[@]}"; do
        echo "${entry%%|*}"
    done | sort -u > "$deps_names"

    if ! cmp -s "$manifest_names" "$deps_names"; then
        echo "[WARN] Manifest is stale relative to deps extracted from CMake."
        echo "[WARN] Run ./scripts/prepare-externals.sh --cmake ${CMAKE_FILE} to refresh it."
        echo ""
    fi

    rm -f "$manifest_names" "$deps_names"
}

# ── --verify mode ────────────────────────────────────────────────────────────
if $DO_VERIFY; then
    verify_filename() {
        local filename="$1"
        local url http_code

        url=$(registry_url "$filename")
        http_code=$(curl -sI -o /dev/null -w "%{http_code}" \
            --header "PRIVATE-TOKEN: ${GITLAB_TOKEN}" "$url")

        if [[ "$http_code" == "200" ]]; then
            echo "  ✓ ${filename}"
            VERIFY_OK=$((VERIFY_OK + 1))
        elif [[ "$http_code" == "404" ]]; then
            echo "  ✗ ${filename}  (MISSING)"
            VERIFY_MISSING=$((VERIFY_MISSING + 1))
        else
            echo "  ? ${filename}  (HTTP ${http_code})"
            VERIFY_ERROR=$((VERIFY_ERROR + 1))
        fi
    }

    VERIFY_OK=0
    VERIFY_MISSING=0
    VERIFY_ERROR=0

    if [[ ${#DEPS[@]} -gt 0 ]]; then
        echo "Verifying deps extracted from CMake against GitLab Package Registry..."
        echo ""
        for entry in "${DEPS[@]}"; do
            filename="${entry%%|*}"
            if [[ -n "$FILTER_NAME" && "$filename" != "$FILTER_NAME" ]]; then
                continue
            fi
            verify_filename "$filename"
        done
    else
        if [[ ! -f "$MANIFEST" ]]; then
            echo "ERROR: Manifest not found: ${MANIFEST}"
            exit 1
        fi

        echo "Verifying manifest entries against GitLab Package Registry..."
        echo ""

        while IFS= read -r line; do
            # Skip comments and blank lines
            [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue

            filename=$(echo "$line" | awk '{print $2}')
            if [[ -z "$filename" ]]; then continue; fi
            if [[ -n "$FILTER_NAME" && "$filename" != "$FILTER_NAME" ]]; then
                continue
            fi

            verify_filename "$filename"
        done < "$MANIFEST"
    fi

    echo ""
    echo "Results: ${VERIFY_OK} OK, ${VERIFY_MISSING} missing, ${VERIFY_ERROR} errors"
    echo ""
    warn_if_manifest_stale

    if [[ $VERIFY_MISSING -gt 0 || $VERIFY_ERROR -gt 0 ]]; then
        exit 1
    fi
    exit 0
fi

# ── Validate filter name against DEPS ────────────────────────────────────────
if [[ -n "$FILTER_NAME" ]]; then
    FOUND=false
    for entry in "${DEPS[@]}"; do
        if [[ "${entry%%|*}" == "$FILTER_NAME" ]]; then
            FOUND=true
            break
        fi
    done
    if ! $FOUND; then
        echo "ERROR: '${FILTER_NAME}' not found in DEPS array"
        echo "Available deps:"
        for entry in "${DEPS[@]}"; do echo "  ${entry%%|*}"; done
        exit 1
    fi
fi

# ── Download + hash (+ upload) ───────────────────────────────────────────────
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

BULK_MODE=true
if [[ -n "$FILTER_NAME" ]]; then
    BULK_MODE=false
fi

# In bulk mode, regenerate the entire manifest
if $BULK_MODE; then
    > "$MANIFEST"
    echo "# externals-manifest.txt — SHA256 hashes for GitLab-hosted tarballs" >> "$MANIFEST"
    echo "# Generated by prepare-externals.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$MANIFEST"
    echo "" >> "$MANIFEST"
fi

echo "Working directory: $WORKDIR"
if $BULK_MODE; then
    echo "Mode: bulk (${#DEPS[@]} deps)"
else
    echo "Mode: single dep (${FILTER_NAME})"
fi
if $DO_UPLOAD; then echo "Upload: enabled"; else echo "Upload: disabled (dry run)"; fi
echo ""

FAIL_COUNT=0
PROCESSED=0

for entry in "${DEPS[@]}"; do
    name="${entry%%|*}"
    url="${entry##*|}"

    # Apply filter
    if [[ -n "$FILTER_NAME" && "$name" != "$FILTER_NAME" ]]; then
        continue
    fi

    PROCESSED=$((PROCESSED + 1))
    echo "Downloading: ${name}"
    echo "  From: ${url}"

    if ! curl -fsSL -o "${WORKDIR}/${name}" "${url}"; then
        echo "  ERROR: download failed"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi

    sha256=$(sha256_cmd "${WORKDIR}/${name}")
    size=$(du -h "${WORKDIR}/${name}" | cut -f1)
    echo "  SHA256: ${sha256}  (${size})"

    if $BULK_MODE; then
        echo "${sha256}  ${name}" >> "$MANIFEST"
    else
        # Single-dep mode: update only this entry in manifest
        if [[ -f "$MANIFEST" ]] && grep -q "  ${name}$" "$MANIFEST"; then
            sed -i.bak "s|^.*  ${name}$|${sha256}  ${name}|" "$MANIFEST"
            rm -f "${MANIFEST}.bak"
            echo "  Manifest: updated existing entry"
        else
            echo "${sha256}  ${name}" >> "$MANIFEST"
            echo "  Manifest: appended new entry"
        fi
    fi

    if $DO_UPLOAD; then
        echo "  Uploading to GitLab..."
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
            --header "PRIVATE-TOKEN: ${GITLAB_TOKEN}" \
            --upload-file "${WORKDIR}/${name}" \
            "$(registry_url "$name")")
        if [[ "$HTTP_CODE" =~ ^2 ]]; then
            echo "  Uploaded OK (HTTP ${HTTP_CODE})"
        else
            echo "  ERROR: upload failed (HTTP ${HTTP_CODE})"
            FAIL_COUNT=$((FAIL_COUNT + 1))
        fi
    fi
    echo ""
done

echo "Manifest: ${MANIFEST}"
echo "Processed: ${PROCESSED}, failures: ${FAIL_COUNT}"

if [[ $FAIL_COUNT -gt 0 ]]; then
    echo "WARNING: ${FAIL_COUNT} failures occurred"
    exit 1
fi
