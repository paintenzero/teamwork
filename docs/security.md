# Security model (Phase 6)

Scope: harden orchestra for a **single self-hosted operator**, not a multi-tenant
SaaS. Multi-tenant concerns (per-tenant isolation, quota fairness, billing) are
noted as future work, not built here.

## Threat model

The core threat is the **flat-network blast radius**: every participant shares one
Redis bus, so before Phase 6 any process that reached Redis could publish or
subscribe anywhere — read another agent's `control:`, forge another's `presence:`,
tail any session — and any process that reached the orchestrator's HTTP/WS could
drive runs. The realistic adversary is a **compromised agent** (a tool a model can
be talked into misusing, or a buggy/hostile agent binary) and an **unauthenticated
client** on the network.

Mitigations shipped in this increment:

| Threat | Mitigation |
|---|---|
| Unauthenticated client reaches the bus | Redis ACL file; `default` user has a password, so no-auth ⇒ `NOAUTH`. |
| Network sniffing of bus traffic | TLS to Redis (rediss:// / `REDIS_TLS=1`); a TLS-only deployment refuses plaintext. |
| Compromised agent reads another agent's chat control | Per-agent ACL grants `&control:<self>` only — `SUBSCRIBE control:<other>` ⇒ `NOPERM`. |
| Compromised agent forges another agent's presence key | `%RW~presence:agent:<self>` only — writing `presence:agent:<other>` ⇒ `NOPERM`. |
| Compromised agent runs destructive commands | `-@dangerous -@admin` ⇒ `FLUSHALL`/`KEYS`/`CONFIG` denied. |
| Unauthenticated client drives the gateway | Bearer token on every orchestrator HTTP/WS request ⇒ `401` without it. |

## Redis ACL design (`redis/users.acl`)

- **`default`** — admin (password), the only account with `@dangerous`/`@admin`
  (ops, migrations, `redis-cli`).
- **`dev`** — broad non-dangerous; the Bus's default identity so the local stack
  runs with auth on but zero per-process config. Used by the smoke test/examples.
- **`orchestrator`, `recorder`** — broad non-dangerous; both legitimately span all
  sessions/agents (the bridge; the persistence reader/archiver).
- **`agent-<id>`** — least privilege: read any presence (discovery) but write only
  its own; subscribe only its own `control:`; use the shared `obs:`/`req:`/`reply:`
  streams and the `presence:events`/`sessions:events` announcement channels.

Per-agent users mean a real deployment regenerates the dev passwords and assigns
each agent its own. Verified: as `agent-writer`, `SUBSCRIBE control:agent-researcher`
and `SET presence:agent:agent-researcher` both return `NOPERM`, while the agent's own
control/presence and a full chat run work end-to-end.

## Known residual gaps (honest, deferred to later Phase 6 increments)

- **Shared announcement channels.** `presence:events` and `sessions:events` are
  broadcast channels any agent may publish to, so a compromised agent could emit a
  *forged* presence/session event. This is transient: the authoritative presence is
  the per-agent **key** (write-protected) plus the TTL heartbeat, which self-corrects;
  `listAgents()` reads keys, not events. Signing or per-agent announcement channels
  would close it.
- **`obs:*` is not per-session scoped.** Agents get RW on all `obs:` streams because
  session ids are dynamic uuids; an agent could in principle write another session's
  trace. The sensitive isolation targeted by the acceptance criteria (`control:`,
  `presence:`) is enforced; obs scoping needs per-session ACL provisioning.
- **Gateway token is a shared bearer secret**, embedded in the web build — it stops
  unauthenticated clients but is not per-user. Session/OIDC auth is the stronger
  form (WS2 lists it as an option) and is the next step if multi-user is needed.
- **LLM provider-key centralization (`pi` `streamProxy`) is not done.** WS1 calls for
  routing provider keys through a proxy with short-lived per-agent tokens so a
  compromised agent never holds a long-lived key. That is a separate service and is
  deferred; today agents read their key from env.

## Rest of Phase 6 (shipped)

Idempotency (3), delivery recovery/DLQ (4), durable control delivery (5),
reconnection/resilience (6), schema-version checks (7), observability (8),
budget enforcement + rate limits (9), and the Redis-vs-RabbitMQ ADR (10) are all
implemented and verified — see `plans/phase-6-task.md`'s as-built note and
`docs/adr/0001-redis-vs-rabbitmq.md`. The only deferred item is the pi `streamProxy`
LLM-key centralization noted above.
