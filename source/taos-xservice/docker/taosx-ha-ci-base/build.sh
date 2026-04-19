#!/bin/bash
# Build and optionally push the taosx-ha-ci-base image
# This only needs to be run when dependencies change

set -e

IMAGE_NAME="taosx-ha-ci-base:latest"

echo "Building base image: $IMAGE_NAME"
docker build -t $IMAGE_NAME -f Dockerfile .

echo "✅ Base image built successfully: $IMAGE_NAME"
echo ""
echo "To push to a registry (optional):"
echo "  docker tag $IMAGE_NAME <registry>/$IMAGE_NAME"
echo "  docker push <registry>/$IMAGE_NAME"
echo ""
echo "Or save as tar for distribution:"
echo "  docker save $IMAGE_NAME | gzip > taosx-ha-ci-base.tar.gz"
