# Copilot Instructions for tsdb-builder

This document provides context and guidelines for AI assistants (Copilot, GitHub Copilot CLI, etc.) when working with the tsdb-builder Docker images.

---

## Quick Context

### What is tsdb-builder?

tsdb-builder provides three Docker images for building TDengine TSDB components:

- **dev**: Development image (GCC 9.3.1, glibc 2.17)
- **core**: Production image (GCC 7.3.1, glibc 2.17, Kylin V10 compatible)
- **others**: Extended image (GCC 14.2.1, glibc 2.28, includes Node.js/JDK/Maven/.NET)

All images are based on manylinux2014 (dev/core) or manylinux_2_28 (others).

---

## Important Changes (May 2026)

### Make 4.2.1 Upgrade

**Background**: 
- dev/core images previously used Make 3.82 (CentOS 7 default)
- Make 3.82 has a known parallel scheduling bug ([PR #12610](https://savannah.gnu.org/support/?109593))
- This bug caused intermittent `ext_curl` configure failures when building TDinternal

**Solution**:
- Upgraded to Make 4.2.1 (compiled from source)
- Make 4.2.1 fixes the parallel scheduling bug
- Fully compatible with glibc 2.17 (manylinux2014)

**Verification**:
```bash
docker run --rm harbor.tdengine.net/tsdb-builder/dev:latest make --version
# Expected: GNU Make 4.2.1
```

### Offline Package Optimization

**What**: 
- Added `scripts/download-packages.sh` to pre-download build dependencies
- Optimized Dockerfiles to use local packages instead of downloading from GitHub

**Benefits**:
- Saves ~15-30 seconds per image build
- More reliable builds (no network dependency during build)
- Better for air-gapped environments

**Usage**:
```bash
# Download packages once
./scripts/download-packages.sh ~/packages

# Use in builds
./build-dev-image.sh --arch amd64 --version 3.4.1 --packages ~/packages
```

---

## Build System Architecture

### Key Files

- `Dockerfile.dev` / `Dockerfile.core` / `Dockerfile.others`: Multi-stage Dockerfiles
- `build.sh`: Unified build script (runs inside Docker containers)
- `build-{dev,core,others}-image.sh`: Image build scripts
- `scripts/download-packages.sh`: Offline package downloader

### Multi-Stage Build Pattern

All Dockerfiles use multi-stage builds:

1. **builder stage**: Architecture variable mapping
2. **make-builder stage** (dev/core only): Compile Make 4.2.1 from source
3. **mold-builder stage**: Compile mold linker
4. **ccache-builder stage**: Compile ccache from source
5. **Main stage**: Final image with all tools

### Package Mounting

Dockerfiles use `--mount=type=bind,from=packages` to access offline packages:

```dockerfile
RUN --mount=type=bind,from=packages,source=.,target=/mnt/packages \
    tar -xzf /mnt/packages/make-4.2.1.tar.gz -C /tmp && \
    cd /tmp/make-4.2.1 && \
    ./configure --prefix=/opt/make --without-guile && \
    make -j$(nproc) && make install
```

This pattern is used for:
- make (dev/core)
- ccache (all images)
- sccache (all images)
- protoc (all images)
- tini (all images)

---

## Common Development Tasks

### Building a New Image

```bash
cd /data/tsdb/tools/tsdb-builder

# Option 1: With offline packages (recommended)
./scripts/download-packages.sh ~/packages
./build-dev-image.sh --arch amd64 --version test --packages ~/packages --local

# Option 2: Without offline packages (slower)
./build-dev-image.sh --arch amd64 --version test --local
```

### Testing Changes

1. Make changes to Dockerfile.dev
2. Build test image: `./build-dev-image.sh --arch amd64 --version test --local`
3. Verify Make version: `docker run --rm <image> make --version`
4. Test full build: `./build.sh --image dev:test --src /data/tsdb ...`

### Modifying Dockerfiles

**Pattern**: When adding/updating tools, follow this pattern:

1. **If tool needs compilation**: Add a builder stage
   ```dockerfile
   FROM stage2-${TARGETARCH} AS tool-builder
   RUN --mount=type=bind,from=packages,source=.,target=/mnt/packages \
       # compile tool
   ```

2. **If tool is binary**: Mount from packages and copy
   ```dockerfile
   RUN --mount=type=bind,from=packages,source=.,target=/mnt/packages \
       cp /mnt/packages/tool-${VERSION} /usr/local/bin/tool && \
       chmod +x /usr/local/bin/tool
   ```

3. **Always verify** in the final stage:
   ```dockerfile
   RUN tool --version
   ```

---

## Troubleshooting Guide for AI Assistants

### ext_curl configure failures

**User says**: "ext_curl build fails with 'cannot compute suffix of executables'"

**Answer**:
1. This is caused by Make 3.82's parallel scheduling bug
2. Solution: Use latest dev/core images (with Make 4.2.1)
3. Workaround: Clear cache and rebuild:
   ```bash
   rm -rf /root/cache/tsdb-builder/externals-*/build/Release/ext_curl
   ```

### Dockerfile build fails: packages not found

**User says**: "Docker build fails with 'no such file or directory' for packages"

**Answer**:
1. User needs to download offline packages first
2. Or omit `--packages` parameter to download during build
   ```bash
   # Download packages
   ./scripts/download-packages.sh ~/packages
   
   # Build with packages
   ./build-dev-image.sh --packages ~/packages
   
   # Or build without packages (slower)
   ./build-dev-image.sh
   ```

### glibc compatibility concerns

**User asks**: "Is Make 4.2.1 compatible with glibc 2.17?"

**Answer**:
Yes, fully compatible. Make 4.2.1 compiled with `--without-guile` only depends on:
- libdl.so.2 (glibc 2.0+)
- libc.so.6 (glibc 2.2.5+)

Verify with: `docker run --rm <image> ldd /usr/bin/make`

---

## Best Practices

### When Modifying Dockerfiles

1. **Test with both architectures** if possible:
   ```bash
   ./build-dev-image.sh --arch amd64,arm64 --version test
   ```

2. **Verify all tools** in the final image:
   ```bash
   docker run --rm <image> bash -c '
     make --version &&
     gcc --version &&
     go version &&
     rustc --version'
   ```

3. **Check image size**: Keep layers minimal, clean up build artifacts

4. **Update README.md**: Document version changes in the tools table

### When Updating Tool Versions

1. Update `ARG` in Dockerfile
2. Update tool table in README.md
3. Update `scripts/download-packages.sh` if needed
4. Test build with new version
5. Commit with conventional commit message:
   ```
   feat(tsdb-builder): upgrade <tool> to <version>
   ```

### When Adding New Packages

1. Add to `scripts/download-packages.sh`
2. Add build stage in Dockerfile if needed
3. Use `--mount=type=bind,from=packages` pattern
4. Update README.md tool table
5. Document in COPILOT-INSTRUCTIONS.md

---

## Commit Message Guidelines

Follow Conventional Commits:

```
feat(tsdb-builder): upgrade Make to 4.2.1 and optimize offline packages in dev image
fix(tsdb-builder): resolve ext_curl parallel build issue
docs(tsdb-builder): update README with Make 4.2.1 upgrade notes
chore(tsdb-builder): update download-packages.sh script
```

---

## References

- Main README: `./README.md`
- Build script: `./build.sh`
- Download script: `./scripts/download-packages.sh`
- Dockerfiles: `./Dockerfile.{dev,core,others}`

---

**Last Updated**: 2026-05-27
**Maintainer**: TDengine TSDB Team
