# Builder image naming and Harbor publishing design

## Problem

`build-core-image.sh` and `build-others-image.sh` currently tag images as `tsdb-builder-core:amd64` / `tsdb-builder-others:arm64` and only load them into the local Docker daemon. This has three problems:

1. The naming does not separate image identity, version, and architecture in a registry-friendly way.
2. There is no built-in publishing flow to the internal Harbor registry.
3. `build.sh` still depends on the legacy local names instead of the canonical published names.

The goal is to move the repo to a consistent Harbor-first image naming model while preserving a smooth developer workflow for local builds and `build.sh`.

## Goals

- Publish builder images to fixed Harbor repositories:
  - `harbor.tdengine.net/tsdb-builder/core`
  - `harbor.tdengine.net/tsdb-builder/others`
- Require explicit `--version` for image build/publish scripts.
- Publish both single-arch tags and multi-arch manifest tags.
- Make `build.sh` resolve and run the same naming scheme instead of the old `tsdb-builder-core:<arch>` pattern.
- Prefer local exact-match images in `build.sh`, and pull from Harbor only when needed.
- Prompt the user to log in manually with `docker login harbor.tdengine.net` when push or pull requires authentication.

## Non-goals

- Replacing the two image build scripts with a single new script.
- Introducing automatic Harbor login inside scripts.
- Changing the existing Dockerfile contents beyond what is needed for image tagging and publishing flow.

## Repository model

The repositories are fixed:

- `harbor.tdengine.net/tsdb-builder/core`
- `harbor.tdengine.net/tsdb-builder/others`

No per-run `--repo` override is part of this design.

## Tagging model

For each image family (`core`, `others`) and release version `<version>`, the scripts publish:

### Version-scoped tags

- `<repo>:<version>-amd64`
- `<repo>:<version>-arm64`
- `<repo>:<version>` (multi-arch manifest)

### Moving tags

- `<repo>:latest-amd64`
- `<repo>:latest-arm64`
- `<repo>:latest` (multi-arch manifest)

Example for `core` version `3.4.1`:

- `harbor.tdengine.net/tsdb-builder/core:3.4.1-amd64`
- `harbor.tdengine.net/tsdb-builder/core:3.4.1-arm64`
- `harbor.tdengine.net/tsdb-builder/core:3.4.1`
- `harbor.tdengine.net/tsdb-builder/core:latest-amd64`
- `harbor.tdengine.net/tsdb-builder/core:latest-arm64`
- `harbor.tdengine.net/tsdb-builder/core:latest`

## Publish flow

### Inputs

Each image build script requires:

- `--arch amd64|arm64`
- `--version <version>`
- optional `--packages /path/to/packages`

Backward-compatible positional arch support may remain, but version must be explicit.

### Single-arch build and publish

For a command such as:

```bash
./build-core-image.sh --arch arm64 --version 3.4.1
```

the script will:

1. Build `linux/arm64` from the corresponding Dockerfile.
2. Tag the built image as:
   - `harbor.tdengine.net/tsdb-builder/core:3.4.1-arm64`
   - `harbor.tdengine.net/tsdb-builder/core:latest-arm64`
3. Check that the user is logged in or, if push fails for auth reasons, print:
   - `docker login harbor.tdengine.net`
4. Push the two single-arch tags above.
5. Check whether the sibling version tag for the other architecture already exists in Harbor.
6. If both versioned arch tags exist, create and push:
   - `harbor.tdengine.net/tsdb-builder/core:3.4.1`
   - `harbor.tdengine.net/tsdb-builder/core:latest`

### Consistency rule for `latest`

`latest` and `latest` manifest must only point to a consistent two-arch release.

That means:

- `latest-amd64` is updated when the new amd64 image is pushed.
- `latest-arm64` is updated when the new arm64 image is pushed.
- `latest` is updated only after both `<version>-amd64` and `<version>-arm64` exist for the same `<version>`.

This prevents a mixed manifest such as "amd64 is version N, arm64 is still version N-1".

## Manifest creation strategy

The build scripts remain single-arch producers. Multi-arch tags are assembled only after both architecture-specific tags for the same version are available in Harbor.

The script should:

1. Probe Harbor for the sibling arch version tag.
2. If missing, print a success message for the single-arch publish and a note that manifest tags were not updated yet.
3. If present, create or replace both manifest tags:
   - `<repo>:<version>`
   - `<repo>:latest`

This keeps the user workflow simple: re-running either architecture build script after the second arch is published is enough to complete the manifests.

## `build.sh` integration

`build.sh` must stop using the legacy local-only names and instead resolve the canonical Harbor naming scheme.

### New `--image` syntax

`build.sh` will accept:

- `--image core`
- `--image others`
- `--image core:<version>`
- `--image others:<version>`

Semantics:

- `core` means `core:latest`
- `others` means `others:latest`
- `core:<version>` means an explicit version
- `others:<version>` means an explicit version

### Internal image resolution

`build.sh` will resolve the user-facing selector and `--arch` into an exact single-arch image reference:

| User input | `--arch` | Resolved image |
| --- | --- | --- |
| `--image core` | `amd64` | `harbor.tdengine.net/tsdb-builder/core:latest-amd64` |
| `--image core` | `arm64` | `harbor.tdengine.net/tsdb-builder/core:latest-arm64` |
| `--image core:3.4.1` | `amd64` | `harbor.tdengine.net/tsdb-builder/core:3.4.1-amd64` |
| `--image others:3.4.1` | `arm64` | `harbor.tdengine.net/tsdb-builder/others:3.4.1-arm64` |

`build.sh` will run the resolved single-arch tag directly rather than relying on a manifest tag in local cache.

### Local-first / remote-fallback behavior

Default behavior:

1. Resolve the exact image ref from `--image` and `--arch`.
2. Check whether that exact image exists locally.
3. If it exists locally, run it directly.
4. If it does not exist locally, pull it from Harbor and then run it.

An explicit `--pull-image` flag will force refresh:

- skip local existence short-circuit
- pull the resolved exact tag before running

This preserves fast local reuse while giving CI and debugging flows a deterministic refresh path.

## Authentication behavior

Neither the image build scripts nor `build.sh` will perform interactive Harbor login automatically.

Expected behavior:

- On push authentication failure in image build scripts, print a clear message to run:

```bash
docker login harbor.tdengine.net
```

- On pull authentication failure in `build.sh`, print the same guidance.

The scripts should fail loudly instead of silently continuing.

## CLI and UX updates

### `build-core-image.sh`

Add or enforce:

- `--version <version>` (required)
- `--arch amd64|arm64`
- `--packages /path/to/packages`

Update help text and summary output to show:

- target repository
- version
- architecture
- tags that will be pushed

### `build-others-image.sh`

Apply the same interface and messaging model as `build-core-image.sh`.

### `build.sh`

Update:

- help text
- `--image` parsing
- image resolution logic
- pull behavior
- logging to show resolved exact image reference

Add:

- `--pull-image`

## Error handling

### Image build scripts

- Missing `--version` -> fail immediately with usage guidance.
- Invalid `--arch` -> fail immediately.
- Harbor push auth failure -> print login guidance and fail.
- Version arch tag push succeeds but sibling arch version tag is absent -> treat the build as successful; print that manifest tags are not updated yet.
- Manifest creation failure after successful single-arch push -> keep the single-arch push as successful, but emit a clear warning that `<version>` / `latest` manifest update did not complete.

### `build.sh`

- Invalid `--image` selector format -> fail with clear examples.
- Local image missing and pull fails for auth -> print login guidance and fail.
- Resolved image ref should always be shown in logs so the operator can tell exactly what ran.

## Documentation updates

The following files must be updated together:

- `build-core-image.sh`
- `build-others-image.sh`
- `build.sh`
- `README.md`
- `.github/copilot-instructions.md`

Documentation updates must cover:

- fixed Harbor repositories
- `--version` as a required input for image publishing
- all tag types and their meanings
- `build.sh --image core[:version]` / `others[:version]`
- `--pull-image`
- manual Harbor login expectations

## Recommendation on usage

- Use `latest` for convenience during interactive development.
- Use explicit versions (`core:<version>`, `others:<version>`) for CI, release, and reproducible builds.

`latest` is the convenience entry point; versioned tags are the stable entry point.
