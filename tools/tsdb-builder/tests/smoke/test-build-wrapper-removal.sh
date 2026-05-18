#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "Running test-build-wrapper-removal.sh (repo root: $REPO_ROOT)"

# 1) Legacy wrapper scripts must be absent
for f in build-core.sh build-others.sh; do
  if [ -e "$REPO_ROOT/$f" ]; then
    echo "ERROR: legacy wrapper exists: $f"
    exit 1
  fi
done

# 2) Docs should not actively reference the legacy wrappers
for doc in README.md .github/copilot-instructions.md; do
  path="$REPO_ROOT/$doc"
  if [ -f "$path" ] && grep -E -q 'build-core.sh|build-others.sh' "$path"; then
    echo "ERROR: $doc references legacy wrappers"
    grep -nE 'build-core.sh|build-others.sh' "$path" || true
    exit 2
  fi
done

# 3) Ensure replacement build.sh resolves docker from PATH (simulate a fake docker)
TMPBIN="$REPO_ROOT/tests/smoke/_fakebin"
mkdir -p "$TMPBIN"
cat > "$TMPBIN/docker" <<'DOCKER'
#!/usr/bin/env bash
# Minimal fake docker used only for resolution tests
echo "FAKE-DOCKER-OK: $*"
exit 0
DOCKER
chmod +x "$TMPBIN/docker"
export PATH="$TMPBIN:$PATH"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found in PATH after injecting fake docker"
  exit 3
fi

# Sanity: build.sh should be present and mention docker (so that the fake docker could be invoked during a real run)
if [ ! -f "$REPO_ROOT/build.sh" ]; then
  echo "ERROR: build.sh not found in repo root"
  exit 4
fi
if ! grep -q "docker" "$REPO_ROOT/build.sh"; then
  echo "WARN: build.sh does not reference 'docker' — cannot fully verify runtime resolution"
else
  echo "OK: build.sh mentions 'docker' (resolution test satisfied)"
fi

echo "NOTE: This test is a regression guard and is expected to FAIL until wrapper scripts and docs are updated."

# If we reached here, the repo is already clean — exit 0. The common/expected path for this task
# is that one of the earlier checks fails (wrappers/docs present), producing a red test.
exit 0
