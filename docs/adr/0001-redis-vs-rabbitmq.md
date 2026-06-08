# ADR 0001 — Transport: stay on Redis (vs RabbitMQ)

**Status:** Accepted · **Date:** 2026-06-05 · **Phase:** 6 (WS10) · **Deciders:** orchestra maintainers

## Context

orchestra's bus is a single SDK (`@orchestra/bus`) that hides the transport behind
a small surface: presence, request/reply, observability streams, control, and
session announcements. Phase 6 asks us to re-evaluate the transport — **Redis
Streams + pub/sub (current) vs RabbitMQ** — against the routing and delivery needs
we *actually observed* building Phases 0–6, not hypothetical ones.

What we actually use, and learned:

- **Replayable observability.** `obs:<session>` is an append-only, replayable
  stream the UI tails from start and the recorder archives. This is a log, not a
  queue — Redis Streams fit it natively; on RabbitMQ this is a stream-type queue or
  an external store.
- **Request/reply with load balancing.** `req:<agent>` is a consumer group
  (`workers`); at-least-once with explicit `XACK`. Phase 6 added `XPENDING`/`XCLAIM`
  reclaim and a `req:<agent>:dlq` dead-letter stream by hand.
- **Presence.** TTL'd keys + a heartbeat + a pub/sub announcement channel — a
  natural fit for Redis key TTLs; not a messaging concept at all.
- **Durable, resumable control.** Phase 6 (WS5) moved control to a stream with a
  per-consumer resume position — again a log/stream, done in a few lines.
- **Idempotency.** `SET NX` with TTL — a Redis primitive we already had on hand.
- **One datastore.** Redis is *also* our presence store, idempotency store, and
  control-position store. We run one dependency, not two.

Observed scale: a handful of agents, low message rates, a single self-hosted
operator (per the Phase 6 scope). We have **not** hit the limits that typically
justify RabbitMQ.

## Options

1. **Stay on Redis Streams + pub/sub (with the Phase 6 hand-built reliability:
   consumer groups, `XAUTOCLAIM` reclaim, a DLQ stream, idempotency).**
2. **Move to RabbitMQ** (classic/quorum queues, native DLX/dead-letter, per-message
   TTL, rich routing/topic exchanges, mature management UI + Prometheus plugin).

### Trade-offs

| Dimension | Redis Streams (current) | RabbitMQ |
|---|---|---|
| Replayable event log (obs trace) | Native (`XRANGE`/`XREAD`) | Streams queue or external store |
| Request/reply load balancing | Consumer groups + `XACK` | Work queues + ack |
| Dead-lettering | Hand-built (`req:*:dlq` + attempt count) | Native DLX, less code |
| Redelivery of dead workers | `XPENDING`/`XAUTOCLAIM` (built in WS4) | Native (consumer cancel / requeue) |
| Presence (TTL keys) | Native | Not a queue concept — needs a side store |
| Idempotency / control position | Native (`SET NX`, stream `GET/SET`) | Needs a side store anyway |
| Routing / capability fan-out | Manual (we ship *directed* dispatch; `req:cap:*` is deferred) | Native topic/headers exchanges |
| Operational surface | One dependency, ACL namespacing (Phase 6 WS1) | Second dependency, vhosts/users |
| Delivery guarantees | At-least-once (we made handlers idempotent) | At-least-once, richer QoS knobs |

RabbitMQ's wins are **native DLX**, **richer routing** (which would make the
deferred capability-queue dispatch a first-class exchange), and a **mature
management/metrics story**. Its costs are a **second stateful dependency** (we'd
*still* keep Redis for presence/idempotency/obs-replay, or move those too at real
cost), a second authz model (vhosts/users vs the ACL we just built), and re-doing
the replayable-trace design that Streams gives us for free.

## Decision

**Stay on Redis.** The needs we actually observed — a replayable trace, modest
request/reply with load balancing, TTL presence, idempotency, and a resumable
control log — are all things Redis does natively in one dependency, and Phase 6
closed the two real gaps (reclaim + DLQ, idempotency) with a small amount of
code behind the SDK. RabbitMQ's decisive advantages (native DLX, topic routing)
do not yet pay for a second stateful system at our scale and single-operator scope.

## Consequences

- We own the reliability code we added (reclaim/DLQ, idempotency, durable control).
  It is small, tested, and contained in `@orchestra/bus`.
- **Revisit triggers** — reconsider RabbitMQ (or NATS/JetStream) if we hit any of:
  capability-queue fan-out becomes central (many agents per capability, dynamic
  routing); message volume outgrows a single Redis; we need per-message priorities
  or richer QoS; or DLQ/retry ergonomics become a maintenance burden.
- Because everything is behind `@orchestra/bus`, a future migration stays contained
  to that one package — which is the property that made deferring this safe.
