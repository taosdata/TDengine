# docs-ci Docker Image Design

**Date:** 2026-05-14  
**Status:** Approved

## Goal

Provide a self-contained Docker image for the four docs CI jobs (`check-typos`, `check-autocorrect`, `check-markdownlint`, `build-doc`) so that linting tools do not need to be installed on the GitLab shell runner host.

## Base Image

`node:24-bookworm-slim`

- Node.js 24 satisfies the docs framework requirement of `>=20`.
- Debian Bookworm glibc base: compatible with NAPI.RS linux-x64-gnu native binaries.
- `-slim` variant avoids unnecessary packages; we add only what is needed.

## Tools

| Tool | Install method | Version ARG |
|------|----------------|-------------|
| `markdownlint-cli2` | `npm install -g markdownlint-cli2@VERSION` | `MARKDOWNLINT_VERSION` |
| `autocorrect` | `npm install -g autocorrect-node@VERSION` | `AUTOCORRECT_VERSION` |
| `typos` | Pre-built musl binary from GitHub Releases | `TYPOS_VERSION` |

### Why these methods?

- `autocorrect-node` ships platform-native NAPI.RS binaries via npm optional dependencies (`autocorrect-node-linux-x64-gnu`). Installing globally exposes the `autocorrect` CLI via its `bin: {'autocorrect': 'cli.js'}` entry.
- `typos` is a static musl binary; downloading it avoids any Rust toolchain in the image and is fast.
- `markdownlint-cli2` is a pure Node.js global package; no native compilation needed.

## System Packages (APT)

```
git bash ca-certificates curl openssh-client python3 make g++
```

- `git` — workspace operations in `prepare-workspace.sh` and `build-doc.sh`.
- `python3 make g++` — only as node-gyp build fallback during `yarn install` for native addons in the docs frameworks. Not an app runtime dependency.

## Layer Order (cache-friendly)

1. **APT packages** — changes rarely; heaviest layer, cached longest.
2. **npm globals** — occasionally updated; depends on node/npm layer.
3. **typos binary** — occasionally updated; depends on curl/ca-certificates.

## WORKDIR

`/root/gitlab_doc_ci_work` — matches the `DOCS_CI_WORKDIR` convention and the bind-mount root in `run-in-docker.sh`.

## Version Pinning

All three tool versions are `ARG`s with defaults, making it easy to bump without changing the Dockerfile structure:

```bash
docker build \
  --build-arg TYPOS_VERSION=v1.46.1 \
  --build-arg AUTOCORRECT_VERSION=2.14.0 \
  --build-arg MARKDOWNLINT_VERSION=0.22.1 \
  -t docs-ci:latest \
  tools/ci/docs/
```

## Registry and Tagging

- Image name: `docs-ci`
- Tags: `latest` for current; date-stamped (e.g., `2026-05-14`) for rollback.
- Registry target: to be configured on the internal GitLab instance or private Docker registry at `192.168.0.30`.
- `DOCS_CI_IMAGE` on the runner host must be set to the full image reference (e.g., `registry.example.com/docs-ci:latest`).

## Architecture Support

- `x86_64` (primary CI runner): fully supported.
- `aarch64`: typos musl binary exists; `autocorrect-node-linux-x64-musl` does **not** cover arm64 — ARM64 Linux runners are not supported by `autocorrect-node` today.

## Build and Load on 192.168.0.30

```bash
# Build locally and export
docker build -t docs-ci:latest tools/ci/docs/
docker save docs-ci:latest | gzip > docs-ci-latest.tar.gz

# Load on runner host
scp docs-ci-latest.tar.gz root@192.168.0.30:/tmp/
ssh root@192.168.0.30 'docker load < /tmp/docs-ci-latest.tar.gz'

# Set image env for CI
echo 'export DOCS_CI_IMAGE=docs-ci:latest' >> /root/.bashrc  # on runner host
```
