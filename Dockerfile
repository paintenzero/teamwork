# syntax=docker/dockerfile:1
# Node-apps image (paintezero/orchestra): one image runs agent / orchestrator /
# recorder — the compose picks the service via `command`. Apps run from source via
# tsx (the repo's design); only the libraries in packages/* are compiled to dist.
FROM node:22-slim

ENV PNPM_HOME=/pnpm
ENV PATH=/pnpm:$PATH
ENV CI=1
# Pin pnpm to the version the lockfile was made with (newer pnpm enforces a
# minimum-release-age supply-chain policy that rejects this lockfile).
RUN corepack enable && corepack prepare pnpm@9.12.0 --activate
WORKDIR /app

# Manifests first so `pnpm install` is cached across source changes.
COPY pnpm-workspace.yaml pnpm-lock.yaml package.json tsconfig.base.json ./
COPY packages/protocol/package.json   packages/protocol/
COPY packages/bus/package.json        packages/bus/
COPY packages/store/package.json      packages/store/
COPY apps/agent/package.json          apps/agent/
COPY apps/orchestrator/package.json   apps/orchestrator/
COPY apps/recorder/package.json       apps/recorder/
COPY apps/web/package.json            apps/web/
RUN pnpm install --frozen-lockfile

# Source, then compile the shared libraries (the exports resolve to dist/).
COPY . .
RUN pnpm --filter "./packages/*" build

# Default service; docker-compose overrides per role.
CMD ["pnpm", "orchestrator"]
