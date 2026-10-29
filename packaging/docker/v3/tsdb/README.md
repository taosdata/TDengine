# TDengine TSDB Docker Project

简体中文 | [English](README.md)

This project provides a Dockerized version of the TDengine TSDB application. It contains all the files needed to build, run, and deploy TDengine TSDB applications using Docker. (Use this project for v3.3.8.8 and above)

## Project Structure

```
source/taos-community/packaging/docker/v3/tsdb/
                              # Docker build files for TDengine TSDB v3.3.8.8+
│── Dockerfile.base           # Dockerfile for the base image
│── Dockerfile                # Instructions for building the TDengine TSDB Docker image
│── entrypoint.sh             # Initialization script for the TDengine TSDB application
│── README.md                 # English project documentation
│── README-CN.md              # Chinese project documentation
│── test-docker.sh              # Chinese project documentation



```

## Prerequisites

- Docker: Make sure Docker is installed and running on your machine
- Download the TDengine TSDB Enterprise or TSDB OSS installation package to the `source/taos-community/packaging/docker/v3/tsdb` directory
  - [TDengine TSDB Enterprise Download](https://www.taosdata.com/download-center?product=TDengine+TSDB-Enterprise&version=3.3.8.8&platform=Docker&architecture=x64)
  - [TDengine TSDB OSS Download](https://www.taosdata.com/download-center?product=TDengine+TSDB-OSS&version=3.3.8.8&platform=Docker&architecture=x64)


Enterprise builds for the 3.3.6 release line are supported from 3.3.6.67 onward.
Installation packages support both `.tar.gz` and `.tar.xz`.

For offline delivery, export and load the image without a registry:

```bash
docker save tdengine/tsdb-ee:<version> | gzip > tdengine-tsdb-docker.tar.gz
docker load -i tdengine-tsdb-docker.tar.gz
```

## Build Docker Images

To build the TDengine TSDB Docker image, navigate to the project directory and run the following commands. You can use the `BUILD_ENTERPRISE` parameter to choose whether to build the Enterprise or Community version. The base image is shared by both editions and takes no build arguments.
The example below shows how to build the TDengine TSDB Enterprise image for the arm platform:

**Note**: Please replace `<version>` with the actual version number.

```bash
cd source/taos-community/packaging/docker/v3/tsdb
# Build the base image
docker build -t tdengine/tsdb-base:1.2 \
             -f Dockerfile.base .

# Build the TDengine TSDB Enterprise image
docker build --rm -f Dockerfile \
             --network=host \
             -t tdengine/tsdb-ee:<version> . \
             --build-arg BUILD_ENTERPRISE=1 \
             --build-arg pkgFile=tdengine-tsdb-enterprise-<version>-linux-arm64.tar.xz \
             --build-arg BUILDKIT_SANDBOX_HOSTNAME=localhost

```

`BUILDKIT_SANDBOX_HOSTNAME=localhost` is optional. The Dockerfile uses the installer's
`--image-build` mode when available to avoid persisting build host addresses. For
legacy installers, it removes the generated `fqdn` and restores only known local
service connection fields to `localhost`, so the daemon uses the runtime hostname.

If you want to build the TDengine TSDB Community version, use the following commands:

```bash
cd source/taos-community/packaging/docker/v3/tsdb
# Build the base image
docker build -t tdengine/tsdb-base:1.2 \
             -f Dockerfile.base . 

# Build the TDengine TSDB Community image

docker build --rm -f Dockerfile \
             --network=host \
             -t tdengine/tsdb:<version> . \
             --build-arg pkgFile=tdengine-tsdb-oss-<version>-linux-arm64.tar.xz \
             --build-arg BUILDKIT_SANDBOX_HOSTNAME=localhost

```

### Reducing the pull size with zstd

By default Docker compresses layers with gzip. Building with `zstd` instead
reduces the published (pull) size by roughly 12% at the default level, or 23% at
level 19, with no change to the image contents:

| Compression | Pull size |
| ----------- | --------- |
| gzip (default) | 444 MB |
| zstd, level 3 | 389 MB |
| zstd, level 19 | 339 MB |

This requires `docker buildx` and pushing directly to a registry:

```bash
docker buildx build --rm -f Dockerfile \
             --network=host \
             -t <registry>/tdengine/tsdb-ee:<version> . \
             --build-arg BUILD_ENTERPRISE=1 \
             --build-arg pkgFile=tdengine-tsdb-enterprise-<version>-linux-arm64.tar.gz \
             --build-arg BUILDKIT_SANDBOX_HOSTNAME=localhost \
             --output type=image,compression=zstd,compression-level=19,force-compression=true,push=true
```

**Compatibility**: zstd-compressed layers require Docker 23+ or containerd 1.5+.
Older clients cannot pull them. Keep the default gzip build if you need to
support older clients.

## Image Contents

To keep the image small, the following are **not** included:

| Removed | Size | Reason |
| ------- | ---- | ------ |
| `openjdk-17-jre-headless` | 185 MB | Only needed by the JVM-based taosX plugins below |
| taosX `influxdb`, `opentsdb`, `pspace` plugins | 97 MB | Java plugins requiring the JRE |
| `taos-jdbcdriver-*.jar` | 26 MB | Not used by any component in the image |
| Package docs, man pages, locale sources | 22 MB | Not needed at runtime |

Removing the JRE also drops its dependencies (`libnss3`, `libnspr4`,
`libpcsclite1`, `libpng16`, fonts), so the total saving is larger than the sum
above: the image went from 2.02 GB to 1.46 GB uncompressed, and from 634 MB to
444 MB compressed (339 MB with zstd level 19).

The native taosX `opc` plugin **is** included, as it does not need a JVM.

Notes:

- If you need the InfluxDB, OpenTSDB or PI/pspace taosX ingest routes, or the
  JDBC driver, use an image built with those retained.
- Debugging tools (`gdb`, `valgrind`, `vim-tiny`, `curl`, `netcat`, `net-tools`,
  `iputils-ping`) are deliberately kept.
- The base image tag is `1.2`. A cached `tsdb-base:1.1` will still build
  successfully but produces an image that retains the JRE, so rebuild the base
  image after updating.

## Quick Start

Example for quickly running a container:

```bash
docker run -d \
  -v ~/data/taos/dnode/data:/var/lib/taos \
  -v ~/data/taos/dnode/log:/var/log/taos \
  -p 6030:6030 -p 6041:6041 -p 6043:6043 -p 6060:6060 \
  -p 6044-6049:6044-6049 \
  -p 6044-6045:6044-6045/udp \
  --name tdengine-tsdb \
  tdengine/tsdb-ee:<version>
```

## Health Checks and Password Configuration

The image provides two probe modes through `taos-check`:

- `taos-check startup`: Checks whether TDengine has completed startup.
- `taos-check service`: Checks whether the SQL service is externally available.

`taos-check startup` and `taos-check service` are available starting from version `3.4.1.0`.

`taos-check service` uses the same password source as the container entrypoint:

- `TAOS_ROOT_PASSWORD_FILE`
- `TAOS_ROOT_PASSWORD`
- default password `taosdata`

For password and upgrade behavior, this repository only distinguishes before and after `3.3.8.8`:

- Before `3.3.8.8`, if the root password had already been changed in an older image, you still need to follow the old-image behavior and create the `.docker-entrypoint-root-password-changed` marker file before restarting.
- In `3.3.8.8` and later, you can provide the current password through `TAOS_ROOT_PASSWORD` or `TAOS_ROOT_PASSWORD_FILE`, and the container can be upgraded directly. However, if the root password was changed previously, make sure the deployment configuration still provides the current password before restarting the container, recreating the Pod, or upgrading the image. Otherwise, SQL-based health checks and other components that authenticate with the root account may fail.

## Other Deployment Methods

For more deployment methods, please refer to [TDengine Docker Deployment](https://docs.taosdata.com/operation/deployment/#docker-%E9%83%A8%E7%BD%B2).

## Image validation workflow

This is an All-in-One image. Any resident service launched by the entrypoint exiting, even with status 0, fails the container so its configured restart policy can recover it. Disabled or absent components are excluded. SIGTERM/SIGINT perform graceful shutdown with exit status 0.

Run the lightweight regression suite on Linux with Python 3.8+, Bash and procps before changing the entrypoint, Dockerfile or installer:

```bash
cd source/taos-community/packaging/docker/v3/tsdb
python3 -m unittest -v test_entrypoint test_image_build test_image_lifecycle_harness
```

The `docker-entrypoint-regression` MR job runs this suite automatically. Docker-only MRs may opt into the `ci:docker-only` label to skip engine CI and SQL fuzz on pushes; remove it before adding other code changes. Set `RUN_DOCKER_ENTRYPOINT_TESTS_ONLY=true` in Run pipeline for focused validation, skipping full Linux/macOS CI and SQL fuzz while retaining applicable basic checks. It exercises the real entrypoint functions with isolated child processes, including service exits, startup failure, graceful/forced shutdown, preserved exit status, temporary registration helpers and disabled services.

After building an image from an actual installation package, run acceptance tests on an isolated Linux Docker host:

```bash
python3 test_image_lifecycle.py \
  --image tdengine/tsdb-ee-amd64:3.4.3.6.1026 \
  --edition enterprise \
  --report-dir /tmp/tsdb-image-validation-unique
```

Use `--edition oss` with a community image. The report directory must be new. Tests cover all expected resident services, SQL/data preservation across stop/start, each service exiting, automatic restart/data preservation, and disabled services. No host ports are published. Only processes in uniquely labeled containers created by the test are terminated. Tests remove their own containers and anonymous volumes after each case and preserve logs, image/container metadata and JSON results. They leave the input image intact.

Archive reports, then remove the exact test image tag with `docker image rm`; do not use global prune. Check run-specific resources after build failures too. MR descriptions should record the package URL/SHA256, image tag/ID, validation and cleanup outcomes, and any untested editions or scenarios. Lightweight tests do not replace real-image acceptance tests.

The fast suite also checks harness cleanup after start failures, timeouts, missing containers, report write failures and per-container cleanup errors, including ownership-label protection. Real-image acceptance verifies the tini entrypoint, declared data/log volumes, required executables, taosd shared libraries, and that installed entrypoint and health-check scripts match this checkout. It also deliberately uses a missing entrypoint to verify cleanup of a failed-start container and its anonymous volumes. Build the actual Dockerfile using the package commands above; simulated tests cannot replace a real build.

The existing `test-docker.sh` retains password, single/multi-node upgrade and data compatibility scenarios as extended tests. It is not the default smoke entrypoint: it currently hardcodes old image versions, container names and host paths, and some mismatches only emit warnings. Update versions, isolate resources and strengthen failure assertions before using it for acceptance. Run fast regressions for routine MRs; build and smoke-test real images for releases or Dockerfile/entrypoint changes; add relevant extended scenarios for password, initialization or upgrade changes. Validate editions and architectures separately and report untested configurations explicitly.
