import type { ObservabilityEvent } from "@paintenzero/orchestra-protocol";

/** The renderable content blocks a pi assistant message can hold. */
export type ContentBlock =
  | { type: "text"; text: string }
  | { type: "thinking"; thinking: string }
  | { type: "toolCall"; name: string; arguments: unknown };

/** One rendered row in the transcript, folded from the raw event stream. */
export type TraceEntry =
  | { kind: "user"; text: string; from: string }
  | { kind: "assistant"; content: ContentBlock[] }
  | { kind: "tool"; toolCallId: string; toolName: string; args: unknown; result?: unknown; isError?: boolean }
  | { kind: "artifact"; uri: string; name?: string; contentType?: string };

function contentOf(message: unknown): ContentBlock[] {
  const content = (message as { content?: unknown })?.content;
  return Array.isArray(content) ? (content as ContentBlock[]) : [];
}

/** Concatenated text blocks of a pi message content array (or a bare string). */
function textOf(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .filter((c): c is { type: "text"; text: string } => (c as { type?: string })?.type === "text")
    .map((c) => c.text)
    .join("\n");
}

/**
 * Fold the canonical pi `AgentMessage[]` — exactly what the LLM receives — into
 * the same trace rows `foldEvents` produces, so the settled transcript renders the
 * model's true view in both live and history. Assistant tool calls become their own
 * rows; the matching `toolResult` message fills in the result; a `save_artifact`
 * result also yields an artifact card (its `details` carry the ArtifactRef).
 */
export function foldMessages(messages: unknown[]): TraceEntry[] {
  const entries: TraceEntry[] = [];
  for (const raw of messages) {
    const m = raw as {
      role?: string;
      content?: unknown;
      toolCallId?: string;
      toolName?: string;
      isError?: boolean;
      details?: { uri?: string; contentType?: string; name?: string };
    };
    if (m.role === "user") {
      entries.push({ kind: "user", text: textOf(m.content), from: "user" });
    } else if (m.role === "assistant") {
      const content = contentOf(m);
      entries.push({ kind: "assistant", content });
      for (const c of content) {
        if (c.type === "toolCall") {
          const id = (c as { id?: string }).id ?? "";
          entries.push({ kind: "tool", toolCallId: id, toolName: c.name, args: c.arguments });
        }
      }
    } else if (m.role === "toolResult") {
      const t = entries.find(
        (e): e is Extract<TraceEntry, { kind: "tool" }> =>
          e.kind === "tool" && e.toolCallId === m.toolCallId,
      );
      if (t) {
        t.result = textOf(m.content);
        t.isError = m.isError;
      }
      if (m.toolName === "save_artifact" && m.details?.uri) {
        entries.push({
          kind: "artifact",
          uri: m.details.uri,
          contentType: m.details.contentType,
          name: m.details.name,
        });
      }
    }
  }
  return entries;
}

/**
 * Fold the ordered ObservabilityEvent stream into a list of trace rows plus the
 * latest agent status. Assistant text/thinking come from the reconstructed
 * `message` snapshot on each message_* event; tool calls come from the
 * tool_execution_* pair.
 */
export function foldEvents(events: ObservabilityEvent[]): {
  entries: TraceEntry[];
  status?: { status: string; detail?: string };
} {
  const entries: TraceEntry[] = [];
  let active: Extract<TraceEntry, { kind: "assistant" }> | null = null;
  let status: { status: string; detail?: string } | undefined;

  for (const ev of events) {
    switch (ev.type) {
      case "user_message":
        entries.push({ kind: "user", text: ev.text, from: ev.from });
        active = null;
        break;
      case "message_start":
        // Only assistant messages belong in the trace; skip the user-prompt echo.
        if ((ev.message as { role?: string })?.role === "assistant") {
          active = { kind: "assistant", content: contentOf(ev.message) };
          entries.push(active);
        } else {
          active = null;
        }
        break;
      case "message_update":
        if (active) active.content = contentOf(ev.message);
        break;
      case "message_end":
        if (active) active.content = contentOf(ev.message);
        active = null;
        break;
      case "tool_execution_start":
        entries.push({ kind: "tool", toolCallId: ev.toolCallId, toolName: ev.toolName, args: ev.args });
        break;
      case "tool_execution_end": {
        const t = entries.find(
          (e): e is Extract<TraceEntry, { kind: "tool" }> =>
            e.kind === "tool" && e.toolCallId === ev.toolCallId,
        );
        if (t) {
          t.result = ev.result;
          t.isError = ev.isError;
        }
        break;
      }
      case "artifact":
        entries.push({ kind: "artifact", uri: ev.uri, name: ev.name, contentType: ev.contentType });
        active = null;
        break;
      case "status":
        status = { status: ev.status, detail: ev.detail };
        break;
    }
  }
  return { entries, status };
}
