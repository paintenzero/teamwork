#!/usr/bin/env bash
# Build (and push) the multi-arch images for the paintezero org.
#
#   docker login                       # as paintezero (or a member)
#   ./scripts/build-images.sh          # buildx amd64+arm64, --push to the registry
#
# Local single-arch test build (loads into the local docker, no push):
#   OUTPUT=--load PLATFORMS=linux/amd64 ./scripts/build-images.sh
#
# Knobs: ORG, TAG, PLATFORMS, OUTPUT, ORCHESTRA_BUILDER
set -euo pipefail

ORG="${ORG:-paintezero}"
TAG="${TAG:-latest}"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"
OUTPUT="${OUTPUT:---push}"            # default pushes; multi-arch can't be --load'ed
BUILDER="${ORCHESTRA_BUILDER:-orchestra-builder}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# A docker-container builder is required for multi-platform builds.
if ! docker buildx inspect "$BUILDER" >/dev/null 2>&1; then
  echo ">> creating buildx builder '$BUILDER' (docker-container driver)"
  docker buildx create --name "$BUILDER" --driver docker-container --bootstrap >/dev/null
fi

build() {
  local name="$1" dockerfile="$2"
  echo ">> building ${ORG}/${name}:${TAG} for ${PLATFORMS}"
  docker buildx build --builder "$BUILDER" \
    --platform "$PLATFORMS" \
    -f "$dockerfile" \
    -t "${ORG}/${name}:${TAG}" \
    $OUTPUT \
    "$ROOT"
}

build orchestra     "$ROOT/Dockerfile"
build orchestra-web "$ROOT/apps/web/Dockerfile"

echo ">> done: ${ORG}/orchestra:${TAG}, ${ORG}/orchestra-web:${TAG}"
