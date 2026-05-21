#!/bin/bash
# ============================================================================
# Create GitLab mirror repositories for Rust git dependencies
#
# Usage: ./setup-rust-git-mirrors.sh
#
# Creates mirror projects under git.tdengine.net/taosdata/rust-deps/
# using the GitLab API. Requires GITLAB_TOKEN env var with api scope.
#
# After running this script, set up scheduled mirroring in GitLab UI or
# use CI pipeline to periodically pull from upstream.
# ============================================================================

set -euo pipefail

GITLAB_HOST="git.tdengine.net"
GITLAB_GROUP="taosdata/rust-deps"
GITLAB_API="https://${GITLAB_HOST}/api/v4"

if [[ -z "${GITLAB_TOKEN:-}" ]]; then
    echo "ERROR: GITLAB_TOKEN env var required (with api scope)"
    exit 1
fi

# Rust git dependencies to mirror (excluding self-references like taos-connector-rust)
# Format: "name|upstream-url" (bash 3.x compatible — no associative arrays)
MIRRORS=(
    "ring|https://github.com/taosdata/ring.git"
    "tokio-tungstenite|https://github.com/taosdata/tokio-tungstenite.git"
    "multi_index_map|https://github.com/acerDebugman/multi_index_map.git"
    "rust-jemalloc-pprof|https://github.com/polarsignals/rust-jemalloc-pprof.git"
    "TinyTemplate|https://github.com/bitcapybara/TinyTemplate.git"
    "geos|https://github.com/georust/geos.git"
)

# Resolve group ID
echo "[INFO] Resolving group: ${GITLAB_GROUP}"
GROUP_ID=$(curl -sf -H "PRIVATE-TOKEN: ${GITLAB_TOKEN}" \
    "${GITLAB_API}/groups/$(echo "${GITLAB_GROUP}" | sed 's|/|%2F|g')" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['id'])" 2>/dev/null || true)

if [[ -z "${GROUP_ID}" ]]; then
    echo "[INFO] Group not found. Creating: ${GITLAB_GROUP}"
    PARENT_ID=$(curl -sf -H "PRIVATE-TOKEN: ${GITLAB_TOKEN}" \
        "${GITLAB_API}/groups/taosdata" | \
        python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
    GROUP_ID=$(curl -sf -X POST -H "PRIVATE-TOKEN: ${GITLAB_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"name\": \"rust-deps\", \"path\": \"rust-deps\", \"parent_id\": ${PARENT_ID}, \"visibility\": \"internal\"}" \
        "${GITLAB_API}/groups" | \
        python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
    echo "[INFO] Created group ID: ${GROUP_ID}"
fi

echo "[INFO] Group ID: ${GROUP_ID}"
echo ""

CREATED=0
SKIPPED=0

for entry in "${MIRRORS[@]}"; do
    name="${entry%%|*}"
    upstream="${entry#*|}"
    echo "[INFO] Processing: ${name} ← ${upstream}"

    # Check if project already exists
    existing=$(curl --max-time 30 -s -H "PRIVATE-TOKEN: ${GITLAB_TOKEN}" \
        "${GITLAB_API}/groups/${GROUP_ID}/projects?search=${name}" | \
        python3 -c "import sys,json; projects=json.load(sys.stdin); print(len([p for p in projects if p['path']=='${name}']))" 2>/dev/null || echo "0")

    if [[ "${existing}" != "0" ]]; then
        echo "  [SKIP] Already exists"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Create project with import_url for initial mirror
    # Note: GitLab import can take time; use longer timeout
    echo "  Creating project (may take a moment)..."
    result=$(curl --max-time 120 -s -X POST -H "PRIVATE-TOKEN: ${GITLAB_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{
            \"name\": \"${name}\",
            \"path\": \"${name}\",
            \"namespace_id\": ${GROUP_ID},
            \"visibility\": \"internal\",
            \"import_url\": \"${upstream}\",
            \"mirror\": true,
            \"mirror_trigger_builds\": false
        }" \
        "${GITLAB_API}/projects" 2>&1)

    if echo "${result}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id',''))" 2>/dev/null | grep -q '^[0-9]'; then
        echo "  [OK] Created (import may still be running in background)"
        CREATED=$((CREATED + 1))
    else
        echo "  [WARN] Failed: ${result}"
    fi
done

echo ""
echo "[INFO] Done: ${CREATED} created, ${SKIPPED} skipped"
echo ""
echo "Mirror URLs for Cargo.toml:"
echo "  https://${GITLAB_HOST}/${GITLAB_GROUP}/ring.git"
echo "  https://${GITLAB_HOST}/${GITLAB_GROUP}/tokio-tungstenite.git"
echo "  https://${GITLAB_HOST}/${GITLAB_GROUP}/multi_index_map.git"
echo "  https://${GITLAB_HOST}/${GITLAB_GROUP}/rust-jemalloc-pprof.git"
echo "  https://${GITLAB_HOST}/${GITLAB_GROUP}/TinyTemplate.git"
echo "  https://${GITLAB_HOST}/${GITLAB_GROUP}/geos.git"
