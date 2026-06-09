/**
 * Observability events — the per-session trace the web UI and curious agents
 * watch. The first ten members mirror @earendil-works/pi-agent-core's
 * `AgentEvent` exactly, so an agent forwards `agent.subscribe(...)` output with
 * zero translation. The last three are orchestra additions.
 */
export type AgentStatus = "idle" | "busy" | "error" | "offline";

export type ObservabilityEvent =
  | { type: "agent_start" }
  | { type: "agent_end"; messages: unknown[] }
  | { type: "turn_start" }
  | { type: "turn_end"; message: unknown; toolResults: unknown[] }
  | { type: "message_start"; message: unknown }
  | { type: "message_update"; message: unknown; assistantMessageEvent: unknown }
  | { type: "message_end"; message: unknown }
  | { type: "tool_execution_start"; toolCallId: string; toolName: string; args: unknown }
  | { type: "tool_execution_update"; toolCallId: string; toolName: string; args: unknown; partialResult: unknown }
  | { type: "tool_execution_end"; toolCallId: string; toolName: string; result: unknown; isError: boolean }
  // orchestra additions:
  | { type: "user_message"; text: string; from: string } // the user/peer turn, recorded for replay
  | { type: "artifact"; uri: string; contentType?: string; name?: string }
  | { type: "status"; status: AgentStatus; detail?: string };

/** Control-plane messages addressed at a single agent's current run. */
export type ControlMessage =
  | { type: "user_message"; text: string }       // chat / peer turn; the agent routes prompt-vs-steer
  | { type: "inject_message"; message: unknown } // -> agent.steer(...)
  | { type: "follow_up"; message: unknown }      // -> agent.followUp(...)
  | { type: "cancel" }                           // -> agent.abort()
  | { type: "ping" };

/** Body of a response envelope produced by onRequest handlers. */
export type ResponseBody<R = unknown> =
  | { ok: true; result: R }
  | { ok: false; error: string };

export interface AgentRegistration {
  id: string;
  /** Natural-language advertisement of what this agent does — the primary way peers
   *  discover it (each agent injects the roster of these into its system prompt). */
  description?: string;
  /** Optional capability tags (kept for filtering/back-compat; discovery is by description). */
  capabilities: string[];
  status: AgentStatus;
  metadata?: Record<string, unknown>;
  startedAt: number;
}

export type PresenceEvent =
  | { event: "register"; registration: AgentRegistration }
  | { event: "update"; registration: AgentRegistration }
  | { event: "deregister"; id: string };

/**
 * Session lifecycle announcement (pub/sub, like presence). The initiator that
 * allocates a `sessionId` announces it (`open`) so a recorder can open a Postgres
 * row and start tailing the session's observability stream. A `rename` carries a
 * human-set display name through to the recorder (the only Postgres writer).
 * Pub/sub is fire-and-forget; the recorder also resumes still-open sessions from
 * Postgres on startup, so a missed `open` is recoverable.
 */
export type SessionEvent =
  | {
      event: "open";
      sessionId: string;
      agentId: string;
      parentSessionId?: string;
      source: string;
      ts: number;
    }
  | {
      event: "rename";
      sessionId: string;
      title: string;
      source: string;
      ts: number;
    };
