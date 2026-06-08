/**
 * Phase 3 recorder. A bus peer (not a UI bridge) whose only job is persistence:
 * it taps the same Redis bus every other participant uses and writes the
 * queryable spine to Postgres + archives full traces to S3.
 *
 *   presence:events    -> upsert `agents` (capabilities, metadata, last_seen)
 *   sessions:events    -> insert `sessions`, then tail that session's obs stream
 *   obs:<sessionId>    -> `messages` (user turn = request, agent_end = response),
 *                         `artifacts`, and on each `status: idle` re-archive the
 *                         whole trace as JSONL to S3 and store the `trace_uri`.
 *
 * Everything is idempotent (message ids are the originating event's messageId,
 * inserted ON CONFLICT DO NOTHING), so re-reading a stream from the start after a
 * restart never duplicates. Open sessions are re-tailed on startup to catch any
 * turns that happened while the recorder was down.
 */
import { readFile } from "node:fs/promises";
import { Bus } from "@paintenzero/orchestra-bus";
import { Store, TraceArchive } from "@paintenzero/orchestra-store";
import type {
  Envelope,
  ObservabilityEvent,
  PresenceEvent,
  SessionEvent,
} from "@paintenzero/orchestra-protocol";

const bus = new Bus({ clientId: "recorder" });
const store = new Store();
const archive = new TraceArchive();

/** Concatenated text of the last assistant message in a pi conversation array. */
function lastAssistantText(messages: unknown[]): string {
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i] as { role?: string; content?: unknown };
    if (m?.role !== "assistant") continue;
    const blocks = Array.isArray(m.content) ? m.content : [];
    return blocks
      .filter((c): c is { type: "text"; text: string } => (c as { type?: string })?.type === "text")
      .map((c) => c.text)
      .join("");
  }
  return "";
}

const agentOf = new Map<string, string>(); // sessionId -> agentId
const tails = new Map<string, () => void>(); // sessionId -> unsubscribe

/** Start persisting one session's observability stream (idempotent to re-call). */
function tail(sessionId: string, agentId: string): void {
  agentOf.set(sessionId, agentId);
  if (tails.has(sessionId)) return;
  const stop = bus.subscribeEvents(sessionId, (ev, env) => void onEvent(sessionId, ev, env), {
    from: "start",
  });
  tails.set(sessionId, stop);
}

async function onEvent(
  sessionId: string,
  ev: ObservabilityEvent,
  env: Envelope<ObservabilityEvent>,
): Promise<void> {
  const agentId = agentOf.get(sessionId);
  try {
    switch (ev.type) {
      case "user_message":
        await store.insertMessage({
          id: env.messageId,
          sessionId,
          direction: "request",
          source: ev.from,
          target: agentId,
          payload: { text: ev.text },
        });
        break;
      case "agent_end":
        await store.insertMessage({
          id: env.messageId,
          sessionId,
          direction: "response",
          source: agentId ?? "agent",
          payload: { text: lastAssistantText(ev.messages) },
        });
        break;
      case "artifact":
        await store.insertArtifact({
          id: env.messageId,
          sessionId,
          s3Uri: ev.uri,
          contentType: ev.contentType,
          name: ev.name,
        });
        break;
      case "status":
        // End of a run: snapshot the full trace to S3 and update the pointer.
        if (ev.status === "idle") await archiveTrace(sessionId);
        break;
    }
  } catch (err) {
    console.error(`[recorder] persist failed (${ev.type}, ${sessionId.slice(0, 8)}…):`, err);
  }
}

async function archiveTrace(sessionId: string): Promise<void> {
  const envs = await bus.readEvents(sessionId);
  if (envs.length === 0) return;
  // WS9: coalesce streaming deltas out of the archive. `message_update` events are
  // incremental snapshots for the live UI; the durable trace keeps message_start/
  // _end (which carry the final message), so dropping the updates shrinks the
  // archive a lot without losing any settled content.
  const coalesced = envs.filter((e) => e.payload.type !== "message_update");
  const uri = await archive.putTrace(sessionId, coalesced);
  await store.setTrace(sessionId, uri);
}

async function onSession(ev: SessionEvent): Promise<void> {
  if (ev.event !== "open") return;
  try {
    await store.ensureAgent(ev.agentId); // satisfy sessions.agent_id FK
    await store.insertSession({
      id: ev.sessionId,
      agentId: ev.agentId,
      parentSessionId: ev.parentSessionId,
    });
    tail(ev.sessionId, ev.agentId);
  } catch (err) {
    console.error("[recorder] session open failed:", err);
  }
}

async function onPresence(ev: PresenceEvent): Promise<void> {
  try {
    if (ev.event === "deregister") {
      await store.touchAgent(ev.id);
      return;
    }
    const r = ev.registration;
    await store.upsertAgent({ id: r.id, capabilities: r.capabilities, metadata: r.metadata });
  } catch (err) {
    console.error("[recorder] presence persist failed:", err);
  }
}

async function main() {
  const schemaUrl = new URL("../../../db/init.sql", import.meta.url);
  await store.applySchema(await readFile(schemaUrl, "utf8"));
  await archive.ensureBucket();

  bus.watchPresence((ev) => void onPresence(ev));
  bus.watchSessions((ev) => void onSession(ev));

  // Resume: re-tail sessions left open by a previous run (replay catches up).
  for (const s of await store.listOpenSessions()) tail(s.id, s.agentId);

  console.log("[recorder] online — persisting agents, sessions, messages; archiving traces to S3.");

  let closing = false;
  const shutdown = async () => {
    if (closing) return;
    closing = true;
    console.log("\n[recorder] shutting down...");
    for (const stop of tails.values()) stop();
    await bus.close();
    await store.close();
    archive.close();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("[recorder] fatal:", err);
  process.exit(1);
});
