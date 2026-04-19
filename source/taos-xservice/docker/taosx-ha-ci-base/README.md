# taosx-ha-ci-base

Base Docker image for taosx-ha-ci with all system dependencies pre-installed.

## Purpose

This base image contains all the apt packages and system dependencies needed for the taosx-ha-ci image. By separating these into a base image, we significantly speed up CI builds since apt packages only need to be installed once.

## Contents

- Ubuntu 22.04 base
- System packages: wget, curl, jq, sqlite3, openjdk-18-jre, ca-certificates, locales, tzdata, netcat
- Locale configured: en_US.UTF-8

## Building the Base Image

The base image only needs to be built once (or when dependencies change):

```bash
cd docker/taosx-ha-ci-base
./build.sh
```

Or manually:

```bash
docker build -t taosx-ha-ci-base:latest .
```

## CI Integration

The GitHub Actions workflow automatically:
1. Checks if the base image exists on the runner
2. Builds it only if it doesn't exist
3. Reuses the existing image for subsequent builds

This means the first PR build will be slower (building base image), but all subsequent builds will be much faster.

## Updating Dependencies

If you need to add or update system dependencies:

1. Edit `Dockerfile` to add/modify packages
2. Build the new base image on all CI runners:
   ```bash
   cd docker/taosx-ha-ci-base
   docker build -t taosx-ha-ci-base:latest .
   ```
3. Or delete the old image and let CI rebuild it:
   ```bash
   docker rmi taosx-ha-ci-base:latest
   ```

## Distribution (Optional)

If you want to distribute the base image to multiple runners:

### Option 1: Docker Registry
```bash
docker tag taosx-ha-ci-base:latest <registry>/taosx-ha-ci-base:latest
docker push <registry>/taosx-ha-ci-base:latest
```

Then update the Dockerfile to use:
```dockerfile
FROM <registry>/taosx-ha-ci-base:latest
```

### Option 2: Save as tar
```bash
docker save taosx-ha-ci-base:latest | gzip > taosx-ha-ci-base.tar.gz
# Copy to other runners
docker load < taosx-ha-ci-base.tar.gz
```

## Size

The base image is approximately 500-600MB, which is cached locally on each runner.
