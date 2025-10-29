#!/usr/bin/env bash

# Purpose: Create and push a lightweight branch to trigger CI without running anything locally.
# Usage:   ./scripts/ci_trigger.sh [branch-name]
# Example: ./scripts/ci_trigger.sh              # creates ci/smoke-YYYYMMDD_HHMMSS
#          ./scripts/ci_trigger.sh ci/smoke-try # uses provided branch name

set -euo pipefail

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "ERROR: Not inside a git repository." >&2
  exit 1
fi

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

timestamp=$(date +%Y%m%d_%H%M%S)
branch_name="${1:-ci/smoke-$timestamp}"

# Marker file to ensure CI triggers without functional code changes
marker_path="plugins/incremental_bitmap/.ci_trigger"
mkdir -p "$(dirname "$marker_path")"
echo "$timestamp" > "$marker_path"

echo "Creating and pushing branch: $branch_name"
git checkout -b "$branch_name"
git add -A
git commit -m "ci: trigger smoke run ($timestamp)"

if git remote -v | grep -q '^origin\s'; then
  git push -u origin "$branch_name"
  echo "Pushed branch to origin: $branch_name"
else
  echo "WARNING: No 'origin' remote configured. Add one and push manually, e.g.:" >&2
  echo "  git remote add origin <your_repo_url>" >&2
  echo "  git push -u origin $branch_name" >&2
fi

cat <<EOF

Next steps:
1) Open a Pull Request from '$branch_name' to your target branch (main/develop).
2) CI will run automatically. For real-environment tests, manually dispatch the workflow with run_real_tests=true.

EOF


