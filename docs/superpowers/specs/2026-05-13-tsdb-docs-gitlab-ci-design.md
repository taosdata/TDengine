# TSDB docs GitLab CI migration design

## Summary

Migrate the current TDengine GitHub Actions docs workflow into the `tsdb` repository as a GitLab parent-child pipeline, and run all four docs jobs inside a prebuilt Docker image instead of depending on host-installed tools. The new design adds a repository-root `.gitlab-ci.yml` as the scheduler entrypoint, keeps the existing `.gitlab/.gitlab-ci.yml` as a separate independent pipeline, moves the docs execution logic into a dedicated child pipeline file at `.gitlab/tsdb-build-docs.yml`, and standardizes the runtime workspace around `/root/gitlab_doc_ci_work/{tsdb,docs.taosdata.com,docs.tdengine.com}`.

## Goals

1. Run the docs checks from `tsdb`, scoped to `source/taos-community/docs`.
2. Preserve the current workflow semantics: `typos`, `autocorrect`, `markdownlint`, and `build-doc`.
3. Replace GitHub Action wrappers with GitLab-native jobs that execute repo-local scripts inside a prebuilt Docker runtime.
4. Use GitLab parent-child pipeline structure so docs CI is isolated from other future subproject pipelines.
5. Run all docs-related jobs on runner tag `ci-docs-runner-u0-3`.
6. Make the new CI path use `tsdb` as the primary repository name instead of `TDengine`.

## Non-goals

1. Do not remove or rewrite the existing `.github/workflows/tdengine-docs-ci.yml` in `TDengine` yet.
2. Do not refactor the existing `.gitlab/.gitlab-ci.yml` pipeline in `tsdb`.
3. Do not expand docs trigger scope beyond `source/taos-community/docs` and the new docs child pipeline file.
4. Do not require migrating the runner itself from shell executor to docker executor for this change.

## Current state

- `TDengine` contains `.github/workflows/tdengine-docs-ci.yml` as the source workflow.
- `tsdb` stores docs under `source/taos-community/docs`.
- `tsdb` already has `.gitlab/.gitlab-ci.yml`, but that file is treated as a separate existing pipeline, not as the new top-level scheduler for this migration.
- The docs subtree already contains local config files such as `.markdownlint-cli2.jsonc` and `typos.toml`.
- The target runner host currently lacks `typos`, `autocorrect`, and `markdownlint-cli2`, so host-native execution is not stable enough for the migrated jobs.
- The docs framework repositories still hardcode `../TDengine` in `assemble.js`, while the new workspace layout needs to use `../tsdb` as the default sibling repository name.

## Chosen architecture

### 1. Parent pipeline entry

Add a new repository-root `.gitlab-ci.yml` that acts as the scheduler entry for newly introduced subproject pipelines.

This root file directly defines the docs trigger job instead of delegating trigger rules to a second include file. That keeps the scheduling path visible in one place while still allowing the docs execution logic to remain isolated in a child file.

### 2. Docs child pipeline

Add `.gitlab/tsdb-build-docs.yml` as the only docs child pipeline file.

This file contains docs jobs only. It does not contain parent trigger logic.

### 3. Containerized job execution

All four docs jobs run from the existing shell runner, but each job executes its real work through `docker run`.

The container image is a prebuilt private `docs-ci` image that contains the required toolchain, including `node`, `yarn`, `git`, `typos`, `autocorrect`, and `markdownlint-cli2`. The image does not contain repository content.

Each job bind-mounts the existing host workspace root `/root/gitlab_doc_ci_work` into the container at the same path. The three required repositories remain sibling directories under that root:

1. `/root/gitlab_doc_ci_work/tsdb`
2. `/root/gitlab_doc_ci_work/docs.taosdata.com`
3. `/root/gitlab_doc_ci_work/docs.tdengine.com`

Keeping the same absolute path inside and outside the container minimizes path-specific branching and lets existing shell scripts operate on mounted working copies instead of copied source trees.

### 4. Framework compatibility boundary

The `tsdb` repository and its CI helper scripts standardize on `tsdb` as the repository directory name and do not retain `TDengine` compatibility behavior.

Compatibility for older `yarn local` and docs assembly flows is limited to the docs framework repositories. Their `assemble.js` entrypoints should resolve the source repository root by first checking for `../tsdb`, then checking for `../TDengine`, and failing immediately if neither exists.

All existing hardcoded `../TDengine` references in those framework entrypoints should be rewritten to use that single resolver, including:

1. source root path selection;
2. git command working directory;
3. include-file path resolution.

This keeps compatibility explicit and localized to one place instead of spreading dual-path logic across GitLab YAML and `tsdb` helper scripts.

### 5. Relationship with the existing `.gitlab/.gitlab-ci.yml`

The current `.gitlab/.gitlab-ci.yml` remains untouched and continues to represent its own existing pipeline behavior.

This migration intentionally does not merge or normalize that file into the new root `.gitlab-ci.yml`; the change stays focused on adding the new docs scheduling path.

## Trigger design

The root `.gitlab-ci.yml` defines one docs trigger job that launches the child pipeline from `.gitlab/tsdb-build-docs.yml`.

The trigger job runs only when either of these paths changes:

1. `source/taos-community/docs/**`
2. `.gitlab/tsdb-build-docs.yml`

It does **not** watch `.gitlab/.gitlab-ci.yml`, and it does **not** watch unrelated root scheduler changes. This is intentional: future changes to other subproject trigger logic must not accidentally start the docs child pipeline.

The accepted trade-off is that edits to unrelated parts of the root scheduler will not exercise the docs child pipeline automatically.

## Child pipeline jobs

The child pipeline in `.gitlab/tsdb-build-docs.yml` contains these jobs:

1. `check-with-typos`
2. `check-with-autocorrect`
3. `check-with-markdownlint`
4. `build-doc`

The first three are lightweight validation jobs. `build-doc` runs after those checks succeed, matching the current workflow intent.

## Containerized implementation rule

The GitLab jobs must not rely on GitHub Actions `uses:` wrappers or any GitHub-specific orchestration behavior.

Instead, each check is implemented through repo-local scripts committed in `tsdb`, executed inside the mounted Docker runtime. The migration preserves behavior, but the implementation becomes native to GitLab execution and no longer depends on runner-local tool installation.

The expected mapping is:

1. `check-with-typos`: run the `typos` check against the docs subtree using the repo's `typos.toml` inside the container.
2. `check-with-autocorrect`: run the underlying autocorrect tool directly inside the container, not through a GitHub Action wrapper.
3. `check-with-markdownlint`: run `markdownlint-cli2` inside the container against the docs subtree using the repo's local config.
4. `build-doc`: run the docs build flow inside the container against the mounted three-repo workspace.

## Runner and execution environment

All four docs jobs, including the lightweight checks, are pinned to runner tag `ci-docs-runner-u0-3`.

Implementation and validation work should use `/root/gitlab_doc_ci_work` as the working directory on the test machine. That directory should contain mounted working copies of `tsdb`, `docs.taosdata.com`, and `docs.tdengine.com`; the container runtime must not embed repository content into the image.

The GitLab job environment needs only enough host capability to pull the private `docs-ci` image and run `docker run` with bind mounts. Tool installation belongs inside the image, not on the host.

## Validation requirements

Validation must cover both trigger scope and execution behavior.

### Trigger validation

Verify that:

1. changing files only under `source/taos-community/docs/**` triggers the docs child pipeline;
2. changing `.gitlab/tsdb-build-docs.yml` triggers the docs child pipeline;
3. changing unrelated scheduler logic does not trigger the docs child pipeline.

### Job validation

Verify that:

1. `check-with-typos` reads the existing docs typo configuration and fails on real typo findings from inside the container;
2. `check-with-autocorrect` runs locally on the GitLab runner through the mounted container runtime and reports findings without relying on GitHub Action behavior;
3. `check-with-markdownlint` reads the existing markdownlint config and reports markdown issues correctly from inside the container;
4. `build-doc` executes successfully on `ci-docs-runner-u0-3` using the mounted three-repo workspace;
5. `yarn local` works when the sibling source repo is named `tsdb`, and the framework compatibility layer still supports older `TDengine` layouts if they are present.

## Trade-offs

### Why the trigger job stays in the root file

This keeps the top-level scheduler explicit and avoids adding another trigger-only YAML file. The user preferred this flatter structure over a second dispatch layer.

### Why the child pipeline stays separate

The child file isolates docs execution logic from root scheduling logic. That keeps future maintenance clearer and avoids mixing parent trigger semantics with child job semantics in one YAML file.

### Why jobs use a prebuilt Docker image

The target runner does not reliably provide the required docs tools on the host, and installing them ad hoc inside every job would make CI slower and harder to reproduce. A prebuilt image keeps the toolchain consistent while still letting the host own the checked-out repositories.

### Why compatibility is only in the docs framework

The new CI path should converge on `tsdb` as the only repository name used by `tsdb` scripts and GitLab YAML. Keeping the dual-name support only in `assemble.js` preserves old docs workflows without reintroducing path branching throughout the new CI implementation.

### Why the existing `.gitlab/.gitlab-ci.yml` is left alone

Changing the current independent pipeline entry would broaden scope from “migrate docs CI” into “restructure tsdb CI entrypoints.” That is deliberately out of scope for this design.
