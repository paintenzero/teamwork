# syntax=docker/dockerfile:1
# Web image (paintezero/orchestra-web): builds the Vite SPA and serves it with
# nginx, which also reverse-proxies /api (+ WebSockets) and the health endpoints to
# the orchestrator. nginx injects the gateway bearer token server-side, so the real
# token never ships in the browser bundle and the image is reusable across deploys.

FROM node:22-slim AS build
ENV PNPM_HOME=/pnpm
ENV PATH=/pnpm:$PATH
ENV CI=1
RUN corepack enable && corepack prepare pnpm@9.12.0 --activate
WORKDIR /app
COPY pnpm-workspace.yaml pnpm-lock.yaml package.json tsconfig.base.json ./
COPY packages/protocol/package.json   packages/protocol/
COPY packages/bus/package.json        packages/bus/
COPY packages/store/package.json      packages/store/
COPY apps/agent/package.json          apps/agent/
COPY apps/orchestrator/package.json   apps/orchestrator/
COPY apps/recorder/package.json       apps/recorder/
COPY apps/web/package.json            apps/web/
RUN pnpm install --frozen-lockfile
COPY . .
# Web imports protocol TYPES; build the libs first, then vite-build the SPA.
RUN pnpm --filter "./packages/*" build && pnpm --filter @orchestra/web build

FROM nginx:alpine
COPY --from=build /app/apps/web/dist /usr/share/nginx/html
COPY apps/web/nginx.conf.template /etc/nginx/templates/default.conf.template
# nginx's entrypoint runs envsubst on templates; restrict it to our two vars so
# nginx's own $variables ($host, $http_upgrade, …) are left untouched.
ENV ORCHESTRATOR_UPSTREAM=http://orchestrator:3001 \
    GATEWAY_TOKEN=dev-gateway-token \
    NGINX_ENVSUBST_FILTER=^(ORCHESTRATOR_UPSTREAM|GATEWAY_TOKEN)$
EXPOSE 80
