# Rust/Go Dependency Cache Strategy Design

## Problem

The C/C++ side of the TSDB build has already been improved through:

- GitLab-hosted ExternalProject tarballs
- `ccache` integration in `tsdb-builder`

But `~/tsdb` still contains substantial Go and Rust code, especially the large Rust
workspace in `source/taos-xservice/`, and current optimization is incomplete:

- Go already benefits from persistent `GOMODCACHE` and `GOCACHE`, but dependency
  fetching still depends on external networks unless local node cache is warm.
- Rust currently has persistent Cargo download caches (`cargo-registry/`,
  `cargo-git/`) mounted by `tsdb-builder`, but no internal dependency source for
  crates and git dependencies.
- Rust build latency remains high for cold CI runners, new developer machines,
  and any node whose local cache was cleared.

The immediate goal of this design is to optimize **dependency acquisition** first,
not compile-time CPU cost. Compile-result caching such as `sccache` remains a
follow-up phase, not the first milestone.

## Current State

### Repositories and build entrypoints

- Main code repository: `~/tsdb`
- Builder repository: `platform/tsdb-builder`

### Relevant current behavior

1. `tsdb-builder/build.sh` already mounts:
   - `go-mod/` to `/root/go/pkg/mod`
   - `cargo-registry/` to `/root/.cargo/registry`
   - `cargo-git/` to `/root/.cargo/git`
2. `cmake/taos-keeper.cmake` already prefers persistent `GOMODCACHE` and
   `GOCACHE` when present.
3. `cmake/taos-xservice.cmake`, `cmake/taos-connector-rust.cmake`, and
   `cmake/taos-connector-python.cmake` invoke Cargo builds but do not currently
   redirect crate resolution to an internal registry or mirror.
4. `source/taos-xservice/Cargo.toml` defines a large workspace with many members
   and a heavy third-party dependency graph, making cold builds expensive.

## Goals

1. Make Go and Rust dependency fetching stable and fast inside the intranet.
2. Reduce first-build and cold-runner latency for developers and CI.
3. Support repeatable builds with minimal or no public-network dependency.
4. Reuse existing internal infrastructure where it fits best: **Nexus** and
   **GitLab**.
5. Preserve the current local node caches in `tsdb-builder` as a second cache
   layer rather than replacing them.

## Non-Goals

1. This phase does not attempt to optimize Rust compiler output caching.
2. This phase does not redesign Rust or Go project structure.
3. This phase does not require every dependency to move into a single platform if
   that increases operational risk.

## Recommended Architecture

### 1. Go dependency path: Nexus as the primary proxy

Use Nexus as the internal `GOPROXY` for all Go builds in `~/tsdb`.

Reasoning:

- Go modules fit naturally into a proxy/cache model.
- Nexus is a better operational fit for central dependency proxying than forcing
  GitLab to play that role.
- Existing per-node `go-mod/` cache remains valuable as a second-level cache.

Target data flow:

`go build / go mod download` -> local `go-mod/` cache -> Nexus Go proxy -> public upstream (only on first miss or preheat)

Applies to at least:

- `source/taos-adapter`
- `source/taos-community/tools/keeper`
- `source/taos-insight`
- `source/taos-connector-go`

### 2. Rust crates path: dedicated Cargo mirror, with GitLab as the practical backing option

Use an internal Cargo crates source for Rust dependencies, but do not assume a
single generic proxy will handle all Cargo behavior cleanly.

Recommended preference order:

1. A dedicated internal Cargo sparse registry mirror/proxy
2. GitLab-hosted Rust crate artifacts plus index/mirror content

Reasoning:

- Cargo has stricter and more nuanced behavior than Go, especially around sparse
  registries, index metadata, and git-based dependencies.
- GitLab is a better fit than ad hoc vendoring across many repositories because
  it centralizes versioned artifacts and access control.
- Existing node-local `cargo-registry/` remains the second-level cache.

Target data flow:

`cargo build` -> local `cargo-registry/` cache -> internal Cargo mirror / GitLab-backed registry -> public upstream (only on first miss or preheat)

Primary consumers:

- `source/taos-xservice`
- `source/taos-connector-rust`
- `source/taos-connector-python/taos-ws-py`

### 3. Rust git dependencies: GitLab mirror repositories

Do not mix Rust git dependencies into the crates mirror strategy.

Instead:

- inventory all Cargo git dependencies
- convert versionable ones to published crate versions where possible
- mirror unavoidable git dependencies into GitLab repositories

Reasoning:

- Cargo registry mirroring and Cargo git dependency handling are different
  problem classes
- keeping git dependencies separate makes failure analysis and policy control
  much clearer

Target data flow:

`cargo build` -> local `cargo-git/` cache -> GitLab mirror repo

## Why not use a single system for everything

### Not “all GitLab”

GitLab is a strong fit for artifact hosting and mirrored repositories, but it is
not the best default answer for Go module proxying.

### Not “all Nexus”

Nexus is a strong fit for central proxying and cache management, but Cargo
compatibility, sparse index behavior, and Rust git-dependency edge cases make an
all-Nexus approach riskier.

### Preferred split

- **Nexus**: Go proxy
- **GitLab or dedicated Cargo mirror**: Rust crates source
- **GitLab**: Rust git mirrors

This split minimizes operational risk and matches current infrastructure
strengths.

## Phased Rollout

### Phase A: unify dependency source configuration

Goal: stop direct public-network dependency fetching during normal builds.

Changes:

- inject `GOPROXY=<internal nexus>` into Go build environments
- inject Cargo internal registry configuration into builder images or build-time
  environment
- ensure `~/tsdb` Go/Rust builds consistently use these settings

Expected outcome:

- cold builds become more stable
- CI no longer relies on unpredictable public network performance

### Phase B: preheat internal dependency stores

Goal: avoid developers and CI runners paying the “first miss” cost.

Changes:

- add scheduled preheat jobs for common Go modules
- add scheduled preheat jobs from Rust lockfiles for:
  - `source/taos-xservice/Cargo.lock`
  - `source/taos-connector-rust/Cargo.lock`
  - `source/taos-connector-python/taos-ws-py/Cargo.lock` if applicable

Expected outcome:

- new runners and new developers usually hit warmed internal caches

### Phase C: govern Rust exception paths

Goal: eliminate hidden fallback paths that still touch public networks.

Changes:

- inventory all Rust git dependencies
- mirror them into GitLab where needed
- identify dependencies that perform network work in `build.rs` or require system
  libraries
- maintain an exceptions list for Rust-specific edge cases

Expected outcome:

- reduced surprise failures under restricted-network CI

### Phase D: optionally add compile-result caching later

Goal: build on the new mirror architecture to solve compile-time cost next.

Deferred follow-up:

- Rust `sccache`
- stronger shared Go build-cache strategy if needed

This phase is intentionally deferred. Dependency-source control should land
first, because it delivers organization-wide stability and is easier to validate.

## Operational Rules

1. **Lockfiles remain authoritative**
   - internal mirrors improve transport, not version governance
2. **No silent fallback in CI/release**
   - CI and release builds should not silently revert to public upstreams
3. **Controlled developer fallback**
   - developers may have an explicit emergency override for temporary diagnosis
4. **Keep local node caches**
   - `go-mod/`, `cargo-registry/`, and `cargo-git/` remain in place as second-level caches

## Validation Strategy

### 1. Cold-start validation

- clear local node caches on a test runner
- build Go and Rust targets using only internal sources
- confirm success

### 2. Restricted-network validation

- disable public-network access for the build environment
- confirm Go and Rust builds still succeed

### 3. Performance validation

Measure at least:

- first build on a fresh runner
- second build on the same runner
- first build on a fresh developer machine after the internal mirrors are warmed

### 4. Coverage validation

Confirm:

- Go dependencies resolve through Nexus
- Rust crates resolve through the internal mirror/registry
- Rust git dependencies resolve through GitLab mirror repos
- no unexpected public-network fetch occurs during standard builds

## Success Criteria

1. Common Go modules are consistently served from Nexus.
2. Rust crate dependencies for `taos-xservice` and `taos-connector-rust` resolve
   from internal sources.
3. Rust git dependencies no longer fetch directly from GitHub during normal CI
   or standard development flows.
4. CI can complete representative Go and Rust builds without needing public
   network access.
5. Cold-build time variance drops noticeably for new runners and new developer
   environments.

## Future Extension

Once this design is stable, the next logical design is a combined strategy:

- keep Go on Nexus proxy + local cache
- keep Rust on internal crates source + GitLab git mirrors + local cache
- add Rust `sccache` on top for compile-result reuse

That follow-up would address CPU-bound rebuild time after the network/dependency
source problem has already been solved.
