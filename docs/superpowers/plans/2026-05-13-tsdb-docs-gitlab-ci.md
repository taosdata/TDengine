# TSDB Docs GitLab CI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the migrated docs CI from `tsdb` through a prebuilt Docker runtime mounted over `/root/gitlab_doc_ci_work`, while keeping old `../TDengine` compatibility only inside the two docs framework repositories.

**Architecture:** `tsdb` owns the GitLab scheduler, child pipeline, Docker wrapper, and repo-local helper scripts. The shell runner remains unchanged and only provides the checked-out workspace plus Docker. The two docs framework repos (`docs.taosdata.com` and `docs.tdengine.com`) add a single repo-root resolver so `yarn ass local` prefers `../tsdb` but still accepts `../TDengine`.

**Tech Stack:** GitLab CI YAML, Bash, Docker, Node.js/Yarn, shell smoke tests

---

## File map

### `tsdb` worktree

- Modify: `.gitlab/tsdb-build-docs.yml` — switch all docs jobs to `docker run` through a repo-local wrapper.
- Modify: `tools/ci/docs/common.sh` — rename `TDENGINE_DIR`/derived paths to `TSDB_DIR`, keep docs sibling directories under `/root/gitlab_doc_ci_work`.
- Modify: `tools/ci/docs/prepare-workspace.sh` — prepare `/root/gitlab_doc_ci_work/tsdb`, not `/root/gitlab_doc_ci_work/TDengine`.
- Create: `tools/ci/docs/run-in-docker.sh` — one wrapper for `docker run --rm`, bind mounts, working directory, and image selection.
- Modify: `tools/ci/docs/check-typos.sh` — continue to run in-container with the `tsdb` path layout.
- Modify: `tools/ci/docs/check-autocorrect.sh` — continue to run in-container with the `tsdb` path layout.
- Modify: `tools/ci/docs/check-markdownlint.sh` — continue to run in-container with the `tsdb` path layout.
- Modify: `tools/ci/docs/build-doc.sh` — continue to drive `yarn ass local` / `yarn build`, but against the `tsdb` sibling layout.
- Modify: `tests/ci/scripts/test-docs-ci-workspace.sh` — assert `/tsdb/docs`, not `/TDengine/docs`.
- Modify: `tests/ci/scripts/test-docs-ci-build.sh` — assert the `tsdb` workspace layout and keep zh/en/examples/unknown/CI-vars-unset coverage.
- Modify: `tests/ci/scripts/test-docs-ci-pipeline.sh` — assert the child jobs call the Docker wrapper rather than host-native scripts.
- Create: `tests/ci/scripts/test-docs-ci-docker.sh` — stub `docker` and verify image, bind mount, working directory, and forwarded command arguments.

### `docs.taosdata.com` worktree

- Modify: `assemble.js` — add a `resolveSourceRepoRoot()` helper that prefers `../tsdb`, falls back to `../TDengine`, and throws if neither exists.
- Create: `tests/resolve-source-root.test.js` — validate the resolver against both sibling names.

### `docs.tdengine.com` worktree

- Modify: `assemble.js` — same resolver pattern as the Chinese docs repo.
- Create: `tests/resolve-source-root.test.js` — validate the resolver against both sibling names.

## Dependency order

1. Task 1 and Task 3 can run in parallel.
2. Task 2 depends on Task 1.
3. Task 4 depends on Tasks 2 and 3.

### Task 1: Normalize the `tsdb` workspace layout and add the Docker wrapper

**Files:**
- Modify: `tools/ci/docs/common.sh`
- Modify: `tools/ci/docs/prepare-workspace.sh`
- Modify: `tests/ci/scripts/test-docs-ci-workspace.sh`
- Create: `tools/ci/docs/run-in-docker.sh`
- Create: `tests/ci/scripts/test-docs-ci-docker.sh`

- [ ] **Step 1: Write the failing layout and Docker-wrapper smoke tests**

```bash
# tests/ci/scripts/test-docs-ci-workspace.sh
test -L "$TMP/work/tsdb/docs"
test ! -e "$TMP/work/TDengine/docs"

# tests/ci/scripts/test-docs-ci-docker.sh
#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin" "$TMP/work/tsdb"

cat >"$TMP/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" > "${CAPTURE_FILE}"
EOF
chmod +x "$TMP/bin/docker"

PATH="$TMP/bin:$PATH" \
CAPTURE_FILE="$TMP/docker.log" \
DOCS_CI_WORKDIR="$TMP/work" \
DOCS_CI_IMAGE="docs-ci:test" \
bash "$ROOT/tools/ci/docs/run-in-docker.sh" bash tools/ci/docs/check-typos.sh

grep -F -- "--rm" "$TMP/docker.log"
grep -F -- "-v $TMP/work:$TMP/work" "$TMP/docker.log"
grep -F -- "-w $TMP/work/tsdb" "$TMP/docker.log"
grep -F -- "docs-ci:test" "$TMP/docker.log"
grep -F -- "bash tools/ci/docs/check-typos.sh" "$TMP/docker.log"
```

- [ ] **Step 2: Run the two smoke tests to verify they fail against the current `TDengine`-based layout**

Run:

```bash
cd /Users/apple/.config/superpowers/worktrees/tsdb/feat/tsdb-docs-gitlab-ci
bash tests/ci/scripts/test-docs-ci-workspace.sh
bash tests/ci/scripts/test-docs-ci-docker.sh
```

Expected: workspace test fails on `tsdb/docs` assertions, and the Docker-wrapper test fails because `tools/ci/docs/run-in-docker.sh` does not exist yet.

- [ ] **Step 3: Implement the minimal `tsdb` path rename and Docker wrapper**

```bash
# tools/ci/docs/common.sh
TSDB_DIR="${DOCS_CI_WORKDIR}/tsdb"
ZH_DOC_DIR="${DOCS_CI_WORKDIR}/docs.taosdata.com"
EN_DOC_DIR="${DOCS_CI_WORKDIR}/docs.tdengine.com"
TSDB_DOCS_DIR="${TSDB_DIR}/source/taos-community/docs"

# tools/ci/docs/prepare-workspace.sh
ensure_repo "${TSDB_REPO_URL}" "${TSDB_DIR}"
if [ -e "${TSDB_DIR}/docs" ]; then
  rm -rf "${TSDB_DIR}/docs"
fi
ln -sfn "${TSDB_DOCS_DIR}" "${TSDB_DIR}/docs"

# tools/ci/docs/run-in-docker.sh
#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
source "${ROOT}/tools/ci/docs/common.sh"
: "${DOCS_CI_IMAGE:?DOCS_CI_IMAGE must be set}"
docker run --rm \
  -v "${DOCS_CI_WORKDIR}:${DOCS_CI_WORKDIR}" \
  -w "${TSDB_DIR}" \
  "${DOCS_CI_IMAGE}" \
  "$@"
```

- [ ] **Step 4: Run the smoke tests again**

Run:

```bash
cd /Users/apple/.config/superpowers/worktrees/tsdb/feat/tsdb-docs-gitlab-ci
bash tests/ci/scripts/test-docs-ci-workspace.sh
bash tests/ci/scripts/test-docs-ci-docker.sh
```

Expected: both scripts exit 0.

- [ ] **Step 5: Commit Task 1**

```bash
cd /Users/apple/.config/superpowers/worktrees/tsdb/feat/tsdb-docs-gitlab-ci
git add tools/ci/docs/common.sh tools/ci/docs/prepare-workspace.sh tools/ci/docs/run-in-docker.sh \
  tests/ci/scripts/test-docs-ci-workspace.sh tests/ci/scripts/test-docs-ci-docker.sh
git commit -m "docs(ci): add docker workspace wrapper" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

### Task 2: Switch the child pipeline and build/lint tests to the Docker wrapper

**Files:**
- Modify: `.gitlab/tsdb-build-docs.yml`
- Modify: `tools/ci/docs/build-doc.sh`
- Modify: `tests/ci/scripts/test-docs-ci-build.sh`
- Modify: `tests/ci/scripts/test-docs-ci-pipeline.sh`

- [ ] **Step 1: Update the pipeline and build smoke tests so they fail until the wrapper is wired in**

```bash
# tests/ci/scripts/test-docs-ci-pipeline.sh
grep -F "bash tools/ci/docs/run-in-docker.sh bash tools/ci/docs/check-typos.sh" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "bash tools/ci/docs/run-in-docker.sh bash tools/ci/docs/check-autocorrect.sh" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "bash tools/ci/docs/run-in-docker.sh bash tools/ci/docs/check-markdownlint.sh" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "bash tools/ci/docs/run-in-docker.sh bash tools/ci/docs/build-doc.sh" "${ROOT}/.gitlab/tsdb-build-docs.yml"

# tests/ci/scripts/test-docs-ci-build.sh
test -L "$TMP/work/tsdb/docs"
test ! -e "$TMP/work/TDengine/docs"
```

- [ ] **Step 2: Run the affected smoke tests to verify they fail**

Run:

```bash
cd /Users/apple/.config/superpowers/worktrees/tsdb/feat/tsdb-docs-gitlab-ci
bash tests/ci/scripts/test-docs-ci-build.sh
bash tests/ci/scripts/test-docs-ci-pipeline.sh
```

Expected: build test still looks for `/TDengine/docs`, and pipeline test still sees host-native `bash tools/ci/docs/*.sh` lines.

- [ ] **Step 3: Wire the child pipeline to Docker and keep `build-doc.sh` on the new layout**

```yaml
# .gitlab/tsdb-build-docs.yml
check-with-typos:
  script:
    - : "${DOCS_CI_IMAGE:?set project/group CI variable DOCS_CI_IMAGE}"
    - bash tools/ci/docs/run-in-docker.sh bash tools/ci/docs/check-typos.sh

check-with-autocorrect:
  script:
    - : "${DOCS_CI_IMAGE:?set project/group CI variable DOCS_CI_IMAGE}"
    - bash tools/ci/docs/run-in-docker.sh bash tools/ci/docs/check-autocorrect.sh

check-with-markdownlint:
  script:
    - : "${DOCS_CI_IMAGE:?set project/group CI variable DOCS_CI_IMAGE}"
    - bash tools/ci/docs/run-in-docker.sh bash tools/ci/docs/check-markdownlint.sh

build-doc:
  script:
    - : "${DOCS_CI_IMAGE:?set project/group CI variable DOCS_CI_IMAGE}"
    - bash tools/ci/docs/run-in-docker.sh bash tools/ci/docs/build-doc.sh
```

```bash
# tools/ci/docs/build-doc.sh
if [ "${zh}" = "true" ]; then
  cd "${ZH_DOC_DIR}"
  git checkout -f master
  git pull origin master
  yarn install
  yarn ass local
  yarn build
fi
```

- [ ] **Step 4: Run the full tsdb smoke-test set**

Run:

```bash
cd /Users/apple/.config/superpowers/worktrees/tsdb/feat/tsdb-docs-gitlab-ci
bash tests/ci/scripts/test-docs-ci-workspace.sh
bash tests/ci/scripts/test-docs-ci-docker.sh
bash tests/ci/scripts/test-docs-ci-build.sh
bash tests/ci/scripts/test-docs-ci-pipeline.sh
```

Expected: all four scripts exit 0.

- [ ] **Step 5: Commit Task 2**

```bash
cd /Users/apple/.config/superpowers/worktrees/tsdb/feat/tsdb-docs-gitlab-ci
git add .gitlab/tsdb-build-docs.yml tools/ci/docs/build-doc.sh \
  tests/ci/scripts/test-docs-ci-build.sh tests/ci/scripts/test-docs-ci-pipeline.sh
git commit -m "docs(ci): run docs jobs in docker" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

### Task 3: Add the `tsdb`/`TDengine` compatibility resolver in both docs framework repos

**Files:**
- Modify: `/root/gitlab_doc_ci_work/docs.taosdata.com/assemble.js`
- Create: `/root/gitlab_doc_ci_work/docs.taosdata.com/tests/resolve-source-root.test.js`
- Modify: `/root/gitlab_doc_ci_work/docs.tdengine.com/assemble.js`
- Create: `/root/gitlab_doc_ci_work/docs.tdengine.com/tests/resolve-source-root.test.js`

- [ ] **Step 1: Add failing resolver tests in both docs repos**

```js
// tests/resolve-source-root.test.js
const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { resolveSourceRepoRoot } = require("../assemble.js");

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "repo-root-"));
const docsRepo = path.join(tmp, "docs.taosdata.com");
const tsdb = path.join(tmp, "tsdb");
const tdengine = path.join(tmp, "TDengine");
fs.mkdirSync(docsRepo);
fs.mkdirSync(tsdb);
assert.strictEqual(resolveSourceRepoRoot(docsRepo), tsdb);
fs.rmSync(tsdb, { recursive: true, force: true });
fs.mkdirSync(tdengine);
assert.strictEqual(resolveSourceRepoRoot(docsRepo), tdengine);
fs.rmSync(tdengine, { recursive: true, force: true });
assert.throws(() => resolveSourceRepoRoot(docsRepo), /expected ..\/tsdb or ..\/TDengine/);
```

- [ ] **Step 2: Run the new tests and confirm they fail before the helper exists**

Run:

```bash
cd /root/gitlab_doc_ci_work/docs.taosdata.com && node tests/resolve-source-root.test.js
cd /root/gitlab_doc_ci_work/docs.tdengine.com && node tests/resolve-source-root.test.js
```

Expected: both commands fail because `assemble.js` does not export `resolveSourceRepoRoot` yet and still hardcodes `../TDengine`.

- [ ] **Step 3: Implement the resolver and replace hardcoded `../TDengine` use sites**

```js
const fs = require("fs");
const path = require("path");

function resolveSourceRepoRoot(baseDir = __dirname) {
  const candidates = ["tsdb", "TDengine"];
  for (const name of candidates) {
    const candidate = path.resolve(baseDir, "..", name);
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  throw new Error("expected ../tsdb or ../TDengine next to the docs repo");
}

const SOURCE_REPO_ROOT = resolveSourceRepoRoot();
const SRC_ROOT_PATH = path.join(SOURCE_REPO_ROOT, "docs", "zh");
module.exports.resolveSourceRepoRoot = resolveSourceRepoRoot;
```

Apply the same pattern in both repos, adjusting the language-specific docs root (`zh` vs `en`) and routing every existing `../TDengine`-based include/git path through `SOURCE_REPO_ROOT`.

- [ ] **Step 4: Run the resolver tests and one real `yarn ass local` in each layout**

Run:

```bash
cd /root/gitlab_doc_ci_work/docs.taosdata.com && node tests/resolve-source-root.test.js
cd /root/gitlab_doc_ci_work/docs.tdengine.com && node tests/resolve-source-root.test.js
cd /root/gitlab_doc_ci_work/docs.taosdata.com && yarn ass local
cd /root/gitlab_doc_ci_work/docs.tdengine.com && yarn ass local
```

Expected: both Node tests exit 0, and both `yarn ass local` commands succeed when `/root/gitlab_doc_ci_work/tsdb` is present.

- [ ] **Step 5: Commit Task 3 in each docs repo**

```bash
cd /root/gitlab_doc_ci_work/docs.taosdata.com
git add assemble.js tests/resolve-source-root.test.js
git commit -m "feat: support tsdb sibling repo" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"

cd /root/gitlab_doc_ci_work/docs.tdengine.com
git add assemble.js tests/resolve-source-root.test.js
git commit -m "feat: support tsdb sibling repo" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

### Task 4: Validate the integrated flow on `192.168.0.30`

**Files:**
- Modify if needed after validation: `.gitlab/tsdb-build-docs.yml`
- Modify if needed after validation: `tools/ci/docs/*.sh`
- Modify if needed after validation: `/root/gitlab_doc_ci_work/docs.taosdata.com/assemble.js`
- Modify if needed after validation: `/root/gitlab_doc_ci_work/docs.tdengine.com/assemble.js`

- [ ] **Step 1: Prepare the remote three-repo workspace and image variable**

Run:

```bash
ssh root@192.168.0.30 'bash -lc '"'"'
  : "${DOCS_CI_IMAGE:?set DOCS_CI_IMAGE on the remote shell before validation}"
  mkdir -p /root/gitlab_doc_ci_work &&
  test -d /root/gitlab_doc_ci_work/tsdb/.git &&
  test -d /root/gitlab_doc_ci_work/docs.taosdata.com/.git &&
  test -d /root/gitlab_doc_ci_work/docs.tdengine.com/.git &&
  docker image inspect "${DOCS_CI_IMAGE}" >/dev/null
'"'"''
```

Expected: exit 0. If any repository or image is missing, stop and fix that specific prerequisite before continuing.

- [ ] **Step 2: Run the tsdb smoke tests on the remote host**

Run:

```bash
ssh root@192.168.0.30 '
  cd /root/gitlab_doc_ci_work/tsdb &&
  bash tests/ci/scripts/test-docs-ci-workspace.sh &&
  bash tests/ci/scripts/test-docs-ci-docker.sh &&
  bash tests/ci/scripts/test-docs-ci-build.sh &&
  bash tests/ci/scripts/test-docs-ci-pipeline.sh
'
```

Expected: all four scripts exit 0.

- [ ] **Step 3: Run the real containerized jobs against the mounted workspace**

Run:

```bash
ssh root@192.168.0.30 'bash -lc '"'"'
  : "${DOCS_CI_IMAGE:?set DOCS_CI_IMAGE on the remote shell before validation}"
  cd /root/gitlab_doc_ci_work/tsdb &&
  bash tools/ci/docs/run-in-docker.sh bash tools/ci/docs/check-typos.sh &&
  bash tools/ci/docs/run-in-docker.sh bash tools/ci/docs/check-autocorrect.sh &&
  bash tools/ci/docs/run-in-docker.sh bash tools/ci/docs/check-markdownlint.sh &&
  bash tools/ci/docs/run-in-docker.sh bash tools/ci/docs/build-doc.sh
'"'"''
```

Expected: commands either succeed or fail with real content findings, but no failure may come from missing host tools or `../TDengine` path assumptions.

- [ ] **Step 4: Run the legacy-layout compatibility check**

Run:

```bash
ssh root@192.168.0.30 '
  rm -rf /tmp/docs-ci-compat &&
  mkdir -p /tmp/docs-ci-compat &&
  cp -R /root/gitlab_doc_ci_work/docs.taosdata.com /tmp/docs-ci-compat/ &&
  cp -R /root/gitlab_doc_ci_work/docs.tdengine.com /tmp/docs-ci-compat/ &&
  cp -R /root/gitlab_doc_ci_work/tsdb /tmp/docs-ci-compat/TDengine &&
  ln -s /tmp/docs-ci-compat/TDengine/source/taos-community/docs /tmp/docs-ci-compat/TDengine/docs &&
  cd /tmp/docs-ci-compat/docs.taosdata.com &&
  yarn ass local &&
  cd /tmp/docs-ci-compat/docs.tdengine.com &&
  yarn ass local
'
```

Expected: both commands succeed, proving the compatibility resolver still accepts a legacy `TDengine` sibling.

- [ ] **Step 5: Commit any final validation-driven fixes in the touched repo(s)**

```bash
cd /Users/apple/.config/superpowers/worktrees/tsdb/feat/tsdb-docs-gitlab-ci
git add .gitlab/tsdb-build-docs.yml tools/ci/docs tests/ci/scripts
git commit -m "docs(ci): finish docker docs validation" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```
