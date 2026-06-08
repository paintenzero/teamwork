/**
 * Per-task session check. Plays the role of a calling agent and drives a worker
 * agent B over the delegation protocol, proving:
 *   1. Multiple request/responses with the SAME taskId share ONE session on B
 *      (B keeps the task's context across turns).
 *   2. A DIFFERENT taskId is a separate session with its own context.
 *   3. `close` finalizes a task.
 * Context persistence across a B restart is checked by the surrounding shell script
 * (the taskctx:* snapshot in Redis).
 *
 * Requires a worker agent online (drafts text, holds per-task context):
 *   AGENT_ID=agent-writer AGENT_FAUX=1 AGENT_FAUX_ROLE=worker \
 *     AGENT_DESCRIPTION="Writes prose." pnpm agent
 */
import { Bus } from "@paintenzero/orchestra-bus";
import { uuidv7, type DelegateRequest, type DelegateResult, type ObservabilityEvent } from "@paintenzero/orchestra-protocol";

const bus = new Bus({ clientId: "task-check" });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const TARGET = process.env.TARGET ?? "agent-writer";

/** One round-trip on a task (taskId fixed = same session on B). */
async function turn(taskId: string, prompt: string): Promise<DelegateResult> {
  return bus.request<DelegateResult>(
    TARGET,
    { prompt, taskId, parentSessionId: "task-check-root", delegationPath: [], budget: {} } satisfies DelegateRequest,
    { sessionId: taskId, timeoutMs: 15_000 },
  );
}

const userTurns = async (sid: string) =>
  (await bus.readEvents(sid)).filter((e) => e.payload.type === "user_message").length;
const assistantTurns = async (sid: string) =>
  (await bus.readEvents(sid)).filter((e) => (e.payload as ObservabilityEvent).type === "agent_end").length;

const taskA = uuidv7();
const taskB = uuidv7();

// 1. Two turns on the SAME task → one session, accumulating context.
await turn(taskA, "first message about task A");
await turn(taskA, "second message about task A");
await sleep(300);
console.log(`task A — same session across 2 turns:`);
console.log(`  user_message events in obs:${taskA.slice(0, 8)} = ${await userTurns(taskA)} (expect 2)`);
console.log(`  agent_end events = ${await assistantTurns(taskA)} (expect 2 — B ran twice in one conversation)`);

// 2. A different taskId → a separate session.
await turn(taskB, "only message about task B");
await sleep(300);
console.log(`task B — separate session:`);
console.log(`  user_message events in obs:${taskB.slice(0, 8)} = ${await userTurns(taskB)} (expect 1)`);

// 3. Close task A.
await bus
  .request(TARGET, { prompt: "", taskId: taskA, close: true, parentSessionId: "task-check-root", delegationPath: [] } satisfies DelegateRequest, {
    sessionId: taskA,
    timeoutMs: 5_000,
  })
  .then(() => console.log(`task A closed OK`))
  .catch((e) => console.log(`close failed: ${String(e)}`));

console.log(`\nTASK_A=${taskA}\nTASK_B=${taskB}`);
await bus.close();
process.exit(0);
