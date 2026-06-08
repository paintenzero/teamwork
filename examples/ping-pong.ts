/**
 * Phase 0 smoke test. Proves the bus end-to-end against a running Redis:
 *   - presence (register + list)
 *   - request / reply with correlation
 *   - control (cancel signal delivery)
 *   - observability (publish + tail, including history replay)
 *
 * Run: pnpm infra:up && pnpm smoke
 */
import { Bus } from "@paintenzero/orchestra-bus";

async function main() {
  // An "agent" that echoes requests and reacts to control signals.
  const agent = new Bus({ clientId: "agent-echo" });
  await agent.register({ id: "agent-echo", capabilities: ["echo"], status: "idle" });

  agent.onRequest(async (payload, meta) => {
    await agent.publishEvent("sess-1", { type: "status", status: "busy" });
    return { echoed: payload, from: meta.source, correlationId: meta.correlationId };
  });

  agent.subscribeControl((msg) => {
    console.log("  [agent] control received:", msg.type);
  });

  // The "orchestrator" side: observes presence, calls the agent, watches events.
  const orch = new Bus({ clientId: "orchestrator" });

  const online = await orch.listAgents();
  console.log("online agents:", online.map((a) => `${a.id} [${a.capabilities.join(",")}]`));

  // Tail the session from the start so we catch the agent's status event.
  const seen: string[] = [];
  const stop = orch.subscribeEvents(
    "sess-1",
    (ev) => {
      seen.push(ev.type);
      console.log("  [event]", JSON.stringify(ev));
    },
    { from: "start" },
  );

  const reply = await orch.request("agent-echo", { hello: "world" }, { timeoutMs: 5000 });
  console.log("reply:", JSON.stringify(reply));

  await orch.publishControl("agent-echo", { type: "cancel" });

  // Demonstrate a timeout against an agent that does not exist.
  try {
    await orch.request("agent-missing", { x: 1 }, { timeoutMs: 500 });
  } catch (err) {
    console.log("expected timeout:", (err as Error).name);
  }

  await new Promise((r) => setTimeout(r, 300)); // let async events flush
  console.log("events seen:", seen);

  stop();
  await agent.deregister();
  await agent.close();
  await orch.close();
  console.log("OK");
}

main().catch((e) => {
  console.error("SMOKE FAILED:", e);
  process.exit(1);
});
