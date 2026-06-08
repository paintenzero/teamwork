import { uuidv7 } from "./ids.js";

/** Bump when the wire format changes incompatibly. Consumers should check it. */
export const SCHEMA_VERSION = 1 as const;

export type MessageType = "request" | "response" | "event" | "control";

/**
 * The single shape that travels on every channel. `payload` is narrowed by
 * `type` at the call sites (events, control, request/response bodies).
 */
export interface Envelope<P = unknown> {
  schemaVersion: number;
  /** uuidv7 — globally unique, time-sortable. */
  messageId: string;
  /** Ties a response to its request, and a whole delegation tree together. */
  correlationId?: string;
  /** The messageId that directly caused this one (for causal chains / cycle detection). */
  causationId?: string;
  type: MessageType;
  /** Stable id of the sender (e.g. "agent-a", "orchestrator", "ui:sess-123"). */
  source: string;
  /** Specific client id, or a capability/topic for queue-style dispatch. */
  target?: string;
  /** The unit of work the UI watches. */
  sessionId?: string;
  /** Stream key the responder must write the reply to (requests only). */
  replyTo?: string;
  /** Epoch milliseconds. */
  ts: number;
  payload: P;
}

export type NewEnvelope<P> = {
  type: MessageType;
  source: string;
  payload: P;
  target?: string;
  correlationId?: string;
  causationId?: string;
  sessionId?: string;
  replyTo?: string;
};

export function makeEnvelope<P>(p: NewEnvelope<P>): Envelope<P> {
  return { schemaVersion: SCHEMA_VERSION, messageId: uuidv7(), ts: Date.now(), ...p };
}

/**
 * Whether a received envelope's schema is one this build understands (Phase 6, WS7).
 * Same major version only — a bump means an incompatible wire change, so consumers
 * reject rather than mis-parse. (Up-conversion of older versions would go here.)
 */
export function isSchemaCompatible(env: { schemaVersion?: number }): boolean {
  return env.schemaVersion === SCHEMA_VERSION;
}
