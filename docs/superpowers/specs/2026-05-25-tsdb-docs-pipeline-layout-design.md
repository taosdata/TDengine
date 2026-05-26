# TSDB Docs Pipeline Layout Design

## Goal

Move all TSDB docs CI/CD implementation assets out of `.gitlab/scripts/` and into a repo-owned tooling directory, while keeping GitLab YAML entrypoints in `.gitlab/`.

## Decision Summary

- Keep `.gitlab-ci.yml`, `.gitlab/tsdb-build-docs.yml`, and `.gitlab/tsdb-deploy-docs.yml` in `.gitlab/`
- Move all docs CI/CD shell scripts, Dockerfile, local validation entrypoint, and related docs into `tools/ci/tsdb-docs-pipeline/`
- Delete the old `.gitlab/scripts/tsdb-docs-ci` and `.gitlab/scripts/tsdb-docs-cd` paths after cutover
- Perform migration and directory-level cleanup in one change, but do not intentionally change runtime behavior

## Why This Layout

The current `.gitlab/scripts/...` placement makes repo-owned tooling look GitLab-owned, even though the same scripts are also used for local validation and general docs workflow orchestration. Moving them under `tools/ci/` makes ownership clearer, improves discoverability for developers, and better reflects that these are repository tools first and GitLab job payloads second.

At the same time, the YAML files should stay in `.gitlab/` because they are still the GitLab pipeline entrypoints and are easiest to maintain there.

## Target Directory Structure

```text
.gitlab-ci.yml
.gitlab/
  tsdb-build-docs.yml
  tsdb-deploy-docs.yml

tools/ci/tsdb-docs-pipeline/
  common.sh
  local-validate.sh
  ci/
    prepare-workspace.sh
    run-in-docker.sh
    build-doc.sh
    check-typos.sh
    check-autocorrect.sh
    check-markdownlint.sh
    autofix.sh
  cd/
    deploy.sh
    build-remote.sh
  docker/
    Dockerfile
  docs/
    README.md
    LOCAL-VALIDATE.md

tests/ci/scripts/
  test-docs-ci-*.sh
  test-docs-cd-notification.sh
```

## File Placement Rules

### Top level

- `common.sh` stays at the top level because it is shared by both CI and CD flows
- `local-validate.sh` stays at the top level because it is the primary human-facing local entrypoint

### `ci/`

Contains scripts used by docs lint/build validation flows:

- workspace preparation
- running inside the docs image
- lint checks
- docs build selection/orchestration
- autofix helpers

### `cd/`

Contains deployment-only scripts:

- host-side deploy orchestration
- remote/container-side docs build used by deploy flow

### `docker/`

Contains the docs CI image build context and Dockerfile so container assets are isolated from shell entrypoints.

### `docs/`

Contains human documentation for local validation and script usage so explanatory files do not clutter execution directories.

## Migration Strategy

### Scope

This change is a path relocation plus directory cleanup. It is not intended to change:

- docs branch selection behavior
- build/deploy semantics
- environment variable contracts
- staging/production routing
- notification behavior beyond path updates required by the move

### Cutover

Use a one-shot cutover:

1. Create `tools/ci/tsdb-docs-pipeline/` and move files into the approved structure
2. Update every YAML reference to the new paths
3. Update every shell `source` / script invocation to the new paths
4. Update tests and docs that assert or document old paths
5. Delete `.gitlab/scripts/tsdb-docs-ci`
6. Delete `.gitlab/scripts/tsdb-docs-cd`

No compatibility wrappers should be kept under `.gitlab/scripts/`.

## Required Reference Updates

The migration must update all path-bearing surfaces consistently:

- `.gitlab-ci.yml` `changes:` rules
- `.gitlab/tsdb-build-docs.yml`
- `.gitlab/tsdb-deploy-docs.yml`
- all moved shell scripts that currently `source` `common.sh`
- all moved shell scripts that invoke sibling scripts
- local validation documentation
- tests under `tests/ci/scripts/`

## Testing Strategy

Run the existing docs CI/CD shell coverage after migration:

- `tests/ci/scripts/test-docs-ci-autofix.sh`
- `tests/ci/scripts/test-docs-ci-build.sh`
- `tests/ci/scripts/test-docs-ci-docker.sh`
- `tests/ci/scripts/test-docs-ci-lint.sh`
- `tests/ci/scripts/test-docs-ci-local-validate.sh`
- `tests/ci/scripts/test-docs-ci-pipeline.sh`
- `tests/ci/scripts/test-docs-ci-workspace.sh`
- `tests/ci/scripts/test-docs-cd-notification.sh`

If relocation exposes a gap, add a focused path smoke test rather than broad new behavior tests.

## Risks and Controls

### Risk: partial path migration

If even one YAML reference, shell `source`, or test assertion still points to `.gitlab/scripts/...`, the move will break in a non-obvious way.

**Control:** treat this as a whole-repo path migration and update all references in one pass.

### Risk: mixing relocation with behavior refactors

If the migration also changes logic, failures will be harder to attribute.

**Control:** keep runtime behavior unchanged unless a direct path fix requires a small mechanical adjustment.

### Risk: directory structure over-design

Adding too many nested folders would make call paths harder to follow.

**Control:** keep only one level of functional subdirectories: `ci/`, `cd/`, `docker/`, and `docs/`.

## Out of Scope

- redesigning docs CI/CD behavior
- changing build/deploy environment semantics
- merging GitLab YAML files into `tools/ci/`
- preserving backward-compatible wrapper paths under `.gitlab/scripts/`
