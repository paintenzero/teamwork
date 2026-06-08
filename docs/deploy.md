# Deploying orchestra (remote server)

Production runs from two multi-arch images (built for `linux/amd64` + `linux/arm64`)
under the **paintezero** org:

- **`paintezero/orchestra`** — the Node apps. One image; the compose picks the role
  (`agent` / `orchestrator` / `recorder`) via `command`.
- **`paintezero/orchestra-web`** — the Vite SPA on nginx. nginx also reverse-proxies
  `/api` (+ WebSockets) and the health endpoints to the orchestrator, and **injects the
  gateway bearer token server-side**, so the real token never ships in the browser.

`docker-compose.prod.yml` wires these together with Redis (ACL-protected), Postgres,
and SeaweedFS (S3). Only the web port is published; everything else stays on the
internal network.

## Build & push the images (multi-arch)

```bash
docker login                      # as paintezero (or an org member)
./scripts/build-images.sh         # buildx amd64+arm64, pushes :latest
# pin a tag / change platforms:
TAG=v1 PLATFORMS=linux/amd64,linux/arm64 ./scripts/build-images.sh
```

The script creates a `docker-container` buildx builder (needed for multi-platform) on
first run. Multi-arch images can't be `--load`ed locally; for a local single-arch test
build use `OUTPUT=--load PLATFORMS=linux/amd64 ./scripts/build-images.sh`.

## Configure

```bash
cp .env.prod.example .env
# edit secrets: GATEWAY_TOKEN, the ORCHESTRA_*/AGENT_* Redis passwords,
# POSTGRES_PASSWORD, S3 keys, and (optionally) a real model key.
node scripts/gen-acl.mjs           # regenerate redis/users.acl to match the passwords
```

Object storage: `docker-compose.prod.yml` bundles SeaweedFS for a self-contained
trial, while `docker-compose.backend.yml` uses an **external** S3-compatible
service — set `S3_ENDPOINT` (+ `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`) in
`.env` to point at it (RustFS, MinIO, AWS S3, …). The apps use path-style
addressing, so any S3-compatible endpoint works.

The defaults in `.env.prod.example` match the committed `redis/users.acl`, so the stack
runs as-is for a trial — **but change every secret for a real deployment.** The Redis
passwords in `.env` and in `redis/users.acl` must match; `gen-acl.mjs` keeps them in
sync (it's the single source of truth — edit `.env`, then re-run it).

## Run

```bash
docker compose -f docker-compose.prod.yml --env-file .env pull
docker compose -f docker-compose.prod.yml --env-file .env up -d
```

Open `http://<server>:${WEB_PORT:-80}`. Health: `GET /healthz`, `GET /readyz`,
metrics `GET /metrics` (all via the web proxy). The browser never sees S3 credentials
or the gateway token.

### Notes

- **Model.** Agents default to the token-free `faux` provider (`AGENT_FAUX=1`). For a
  real model, set `AGENT_FAUX=` (empty) and `ANTHROPIC_API_KEY`, or an OpenAI-compatible
  `OPENAI_BASE_URL`/`OPENAI_MODEL`/`OPENAI_API_KEY` (see `.env.prod.example`).
- **More agents.** Copy the `agent-researcher` service block, give it a distinct
  `AGENT_ID` and its own ACL user/password (`agent-writer`, `agent-producer`,
  `agent-reader`, and `agent-echo` already exist in `redis/users.acl`).
- **TLS to Redis** is available (see `docker-compose.tls.yml` / `REDIS_TLS`), but for a
  single-host deployment where Redis isn't exposed it's optional — the ACL already
  refuses no-auth, and nothing outside the compose network can reach Redis. The certs
  are **not committed** (`redis/tls/` is gitignored — never publish a private key).
  Generate a self-signed set before enabling TLS:

  ```bash
  mkdir -p redis/tls && cd redis/tls
  openssl req -x509 -newkey rsa:2048 -days 3650 -nodes \
    -keyout ca.key -out ca.crt -subj "/CN=orchestra-ca"
  openssl req -newkey rsa:2048 -nodes \
    -keyout redis.key -out redis.csr -subj "/CN=redis"
  openssl x509 -req -in redis.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -days 3650 -out redis.crt
  rm redis.csr
  ```

  Clients trust it via `REDIS_TLS_CA=redis/tls/ca.crt`.
- **HTTPS.** Terminate TLS for the public web port at your edge (a reverse proxy /
  load balancer / `WEB_PORT` behind Caddy or Traefik); the web container speaks plain
  HTTP on the internal network.
