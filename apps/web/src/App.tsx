import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import type {
  AgentRegistration,
  ObservabilityEvent,
  PresenceEvent,
} from "@paintenzero/orchestra-protocol";
import { foldEvents, type ContentBlock, type TraceEntry } from "./trace.js";
import { api, apiUrl, wsUrl } from "./api.js";

/** A durable session as returned by GET /api/sessions (recorder's spine). */
interface SessionSummary {
  id: string;
  agentId: string;
  parentSessionId: string | null;
  status: string;
  traceUri: string | null;
  startedAt: string;
}

/** Apply a presence delta to the current roster. */
function applyPresence(agents: AgentRegistration[], ev: PresenceEvent): AgentRegistration[] {
  if (ev.event === "deregister") return agents.filter((a) => a.id !== ev.id);
  const next = agents.filter((a) => a.id !== ev.registration.id);
  return [...next, ev.registration].sort((a, b) => a.id.localeCompare(b.id));
}

export function App() {
  const [agents, setAgents] = useState<AgentRegistration[]>([]);
  const [selected, setSelected] = useState<string | undefined>();
  const [sessionId, setSessionId] = useState<string | undefined>();
  const [events, setEvents] = useState<ObservabilityEvent[]>([]);
  const [historic, setHistoric] = useState(false); // viewing a past session (read-only)
  const [history, setHistory] = useState<SessionSummary[]>([]);
  const [tree, setTree] = useState<SessionSummary[]>([]); // delegation tree of the open session
  const [input, setInput] = useState("");
  const [connected, setConnected] = useState(false); // presence socket up → footer link health
  const sessionSock = useRef<WebSocket | null>(null);
  const traceRef = useRef<HTMLDivElement | null>(null);

  // Roster: seed from GET, keep live via the presence socket.
  useEffect(() => {
    api("/api/agents")
      .then((r) => r.json())
      .then((list: AgentRegistration[]) => {
        setAgents(list.sort((a, b) => a.id.localeCompare(b.id)));
        setSelected((s) => s ?? list[0]?.id);
      })
      .catch(() => {});

    // WS6: keep the presence socket up across blips — reconnect on unexpected close.
    let sock: WebSocket | null = null;
    let closed = false;
    const connect = () => {
      const ws = new WebSocket(wsUrl("/api/presence/stream"));
      sock = ws;
      ws.onmessage = (m) => {
        const ev = JSON.parse(m.data) as PresenceEvent;
        setAgents((cur) => applyPresence(cur, ev));
      };
      ws.onclose = () => {
        setConnected(false);
        if (!closed) setTimeout(connect, 1000); // reconnect, then reseed below
      };
      ws.onopen = () => {
        setConnected(true);
        // re-seed the roster on (re)connect so we don't miss deltas during the gap
        api("/api/agents")
          .then((r) => r.json())
          .then((list: AgentRegistration[]) => setAgents(list.sort((a, b) => a.id.localeCompare(b.id))))
          .catch(() => {});
      };
    };
    connect();
    return () => {
      closed = true;
      sock?.close();
    };
  }, []);

  // Durable session list for the selected agent (from Postgres via the recorder).
  function loadHistory() {
    const q = selected ? `?agentId=${encodeURIComponent(selected)}` : "";
    api(`/api/sessions${q}`)
      .then((r) => r.json())
      .then((list: SessionSummary[]) => setHistory(list))
      .catch(() => {});
  }
  useEffect(loadHistory, [selected]);

  // Open (or reopen) a LIVE session stream — replays from start, then tails.
  // WS6: on an unexpected drop, reconnect and replay (the page stays put — no full
  // reload). The server stream is from-start and foldEvents is idempotent, so the
  // rebuilt transcript matches.
  function openSession(sid: string) {
    sessionSock.current?.close();
    setEvents([]);
    setHistoric(false);
    setSessionId(sid);
    location.hash = `session=${sid}`;
    const connect = () => {
      const ws = new WebSocket(wsUrl(`/api/sessions/${sid}/stream`));
      const replay: ObservabilityEvent[] = [];
      ws.onmessage = (m) => {
        replay.push(JSON.parse(m.data) as ObservabilityEvent);
        setEvents([...replay]);
      };
      ws.onclose = () => {
        // reconnect only if this is still the active live session
        if (sessionSock.current === ws) setTimeout(connect, 1000);
      };
      sessionSock.current = ws;
    };
    connect();
  }

  // Open a PAST session read-only from the durable transcript (S3 / Redis).
  async function openHistory(sid: string) {
    sessionSock.current?.close();
    sessionSock.current = null;
    const res = await api(`/api/sessions/${sid}/transcript`);
    const { events: evs } = (await res.json()) as { events: ObservabilityEvent[] };
    setEvents(evs);
    setHistoric(true);
    setSessionId(sid);
    location.hash = `session=${sid}&view=history`;
  }

  // On load, reconnect to a session named in the URL hash (refresh → replay).
  useEffect(() => {
    const m = location.hash.match(/session=([^&]+)/);
    if (!m) return;
    if (/view=history/.test(location.hash)) void openHistory(m[1]);
    else openSession(m[1]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const { entries, status } = useMemo(() => foldEvents(events), [events]);
  const running = !historic && status?.status === "busy";
  const selectedAgent = agents.find((a) => a.id === selected);

  // Delegation tree of the open session — refetched when a run settles (a child
  // session may have just been created by an ask_agent delegation).
  useEffect(() => {
    if (!sessionId) {
      setTree([]);
      return;
    }
    api(`/api/sessions/${sessionId}/tree`)
      .then((r) => r.json())
      .then((rows: SessionSummary[]) => setTree(rows))
      .catch(() => {});
  }, [sessionId, status?.status]);

  // Follow the latest turn as the transcript grows — but only when the user is
  // already pinned near the bottom. Otherwise leave their scroll position alone,
  // so they can read back through a long session without being yanked down.
  useEffect(() => {
    const el = traceRef.current;
    if (!el) return;
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 120;
    if (nearBottom) el.scrollTop = el.scrollHeight;
  }, [entries.length]);

  // One input, two gestures: the first message opens a session, the rest continue
  // it. The agent decides prompt-vs-steer; here we only route.
  async function send() {
    const text = input.trim();
    if (!text || historic) return;
    setInput("");
    if (!sessionId) {
      if (!selected) return;
      const res = await api("/api/sessions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ agentId: selected, text }),
      });
      const { sessionId: sid } = (await res.json()) as { sessionId: string };
      openSession(sid);
      setTimeout(loadHistory, 500); // let the recorder write the row, then refresh
    } else {
      await api(`/api/sessions/${sessionId}/messages`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ text }),
      });
    }
  }

  async function stop() {
    if (!sessionId) return;
    await api(`/api/sessions/${sessionId}/cancel`, { method: "POST" });
  }

  // Begin a fresh conversation with the selected agent.
  function newChat() {
    sessionSock.current?.close();
    sessionSock.current = null;
    setEvents([]);
    setHistoric(false);
    setSessionId(undefined);
    location.hash = "";
  }

  return (
    <div className="shell">
      <div className="app">
        <aside className="sidebar">
          <div className="brand">
            <div className="brand-mark">T</div>
            <div>
              <div className="brand-name">TEAMWORK</div>
              <div className="brand-ver label-caps">VERSION 0.5</div>
            </div>
          </div>

          <div className="section-head">
            <Icon name="hub" className="sm" />
            <span className="label-caps">AGENTS</span>
          </div>
          {agents.length === 0 && <p className="muted">none online</p>}
          {agents.map((a) => (
            <label key={a.id} className={`agent ${selected === a.id ? "sel" : ""}`}>
              <input
                type="radio"
                name="agent"
                checked={selected === a.id}
                onChange={() => {
                  setSelected(a.id);
                  newChat(); // a new agent means a new conversation
                }}
              />
              <span className={`dot ${a.status}`} />
              <span className="aid">{a.id}</span>
              <span className="caps">{a.description ?? a.capabilities.join(", ")}</span>
            </label>
          ))}

          <div className="section-head">
            <Icon name="history" className="sm" />
            <span className="label-caps grow">HISTORY</span>
            <button className="refresh" title="refresh" onClick={loadHistory}>
              ↻
            </button>
          </div>
          {history.length === 0 && <p className="muted">no past sessions</p>}
          {history.map((s) => (
            <button
              key={s.id}
              className={`session-item ${sessionId === s.id ? "sel" : ""}`}
              onClick={() => void openHistory(s.id)}
            >
              <span className={`dot ${s.status === "open" ? "idle" : "offline"}`} />
              <span className="sid-short">{s.id.slice(0, 8)}…</span>
              <span className="when">{new Date(s.startedAt).toLocaleTimeString()}</span>
            </button>
          ))}
        </aside>

        <main className="run">
          <div className="topbar">
            <button className="newchat" onClick={newChat} disabled={!sessionId && !historic}>
              <Icon name="add" className="sm" /> New chat
            </button>
            {selected && <span className="with mono-ui">chatting with {selected}</span>}
            <span className="spacer" />
            {!historic && status && (
              <span className={`status ${status.status}`}>
                <span className={`dot ${status.status}`} /> {status.status}
              </span>
            )}
            {sessionId && <span className="sid">session {sessionId.slice(0, 8)}…</span>}
          </div>

          <div className="trace" ref={traceRef}>
            {entries.length === 0 && (
              <p className="muted">No messages yet — pick an agent and say hello below.</p>
            )}
            {entries.map((e, i) => (
              <TraceRow key={i} entry={e} agentName={selected} />
            ))}
          </div>

          {historic && (
            <div className="banner">
              Viewing a past session (read-only). Start a <strong>New chat</strong> to talk.
            </div>
          )}

          <form
            className="composer"
            onSubmit={(e) => {
              e.preventDefault();
              void send();
            }}
          >
            <div className="composer-inner">
              <div className="composer-input">
                <span className="prompt-char">&gt;</span>
                <input
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  placeholder={
                    historic
                      ? "read-only — New chat to talk"
                      : !selected
                        ? "no agent selected"
                        : running
                          ? "Steer the agent…"
                          : "Send a command to the agent…"
                  }
                  disabled={!selected || historic}
                />
              </div>
              <div className="composer-row">
                {running && <p className="steer-note">Steers apply at the next pause.</p>}
                <div className="composer-buttons">
                  {running ? (
                    <>
                      <button
                        type="submit"
                        className="btn steer"
                        disabled={!selected || historic || !input.trim()}
                      >
                        <Icon name="navigation" className="sm" /> Steer
                      </button>
                      <button type="button" className="btn stop" onClick={() => void stop()}>
                        <Icon name="stop_circle" className="sm" /> Stop
                      </button>
                    </>
                  ) : (
                    <button
                      type="submit"
                      className="btn send"
                      disabled={!selected || historic || !input.trim()}
                    >
                      <Icon name="send" className="sm" /> Send
                    </button>
                  )}
                </div>
              </div>
            </div>
          </form>
        </main>

        <aside className="inspector">
          {!selectedAgent ? (
            <p className="empty">Select an agent to inspect.</p>
          ) : (
            <>
              <section className="insp-agent">
                <div className="insp-avatar">
                  <div className="mark">{selectedAgent.id.slice(0, 1)}</div>
                  <div className="pip">
                    <span className={`dot ${selectedAgent.status}`} />
                  </div>
                </div>
                <div>
                  <h2>{selectedAgent.id}</h2>
                  <p className="sub">
                    Status: {!historic && status ? status.status : selectedAgent.status}
                  </p>
                </div>
              </section>

              {selectedAgent.description && (
                <section className="insp-section">
                  <div className="insp-head">
                    <Icon name="badge" className="sm" />
                    <span className="label-caps">DESCRIPTION</span>
                  </div>
                  <div className="meta-cell">
                    <div className="v">{selectedAgent.description}</div>
                  </div>
                </section>
              )}

              {selectedAgent.capabilities.length > 0 && (
                <section className="insp-section">
                  <div className="insp-head">
                    <Icon name="bolt" className="sm" />
                    <span className="label-caps">CAPABILITIES</span>
                  </div>
                  <div className="meta-grid">
                    {selectedAgent.capabilities.map((c) => (
                      <div key={c} className="meta-cell">
                        <div className="v">{c}</div>
                      </div>
                    ))}
                  </div>
                </section>
              )}

              {sessionId && (
                <section className="insp-section">
                  <div className="insp-head">
                    <Icon name="account_tree" className="sm" />
                    <span className="label-caps">DELEGATION TREE</span>
                  </div>
                  <div className="tree">
                    <div className="tree-node-wrap">
                      <div className="tree-card sel">
                        <div className="k">ROOT AGENT</div>
                        <div className="agent-name">{selected}</div>
                      </div>
                      <TreeNodes
                        rootId={sessionId}
                        all={tree}
                        current={sessionId}
                        onOpen={openHistory}
                      />
                    </div>
                  </div>
                </section>
              )}
            </>
          )}
        </aside>
      </div>

      <footer className="statusbar">
        <div className="link-health-wrap" style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <span className={`link-health ${connected ? "live" : "down"}`}>
            <span className={`dot ${connected ? "idle" : "error"}`} />
            LINK HEALTH: {connected ? "LIVE" : "DOWN"}
          </span>
          <span className="sys">{agents.length} agents online</span>
        </div>
      </footer>
    </div>
  );
}

/** Material Symbols glyph. */
function Icon({ name, className = "" }: { name: string; className?: string }) {
  return <span className={`material-symbols-outlined ${className}`}>{name}</span>;
}

/** Nested delegation children of `rootId`, drawn from a flat session list. */
function TreeNodes({
  rootId,
  all,
  current,
  onOpen,
  depth = 0,
}: {
  rootId: string;
  all: SessionSummary[];
  current: string;
  onOpen: (id: string) => void;
  depth?: number;
}) {
  const children = all.filter((s) => s.parentSessionId === rootId);
  if (children.length === 0) return null;
  return (
    <div className="tree-children">
      {children.map((c) => (
        <div key={c.id} className="tree-node-wrap">
          <button
            className={`tree-card child ${current === c.id ? "sel" : ""}`}
            onClick={() => onOpen(c.id)}
          >
            <div className="k">DELEGATE</div>
            <div className="agent-name">{c.agentId}</div>
            <div className="sid-short">{c.id.slice(0, 8)}…</div>
          </button>
          <TreeNodes rootId={c.id} all={all} current={current} onOpen={onOpen} depth={depth + 1} />
        </div>
      ))}
    </div>
  );
}

/**
 * An artifact produced during the run (Phase 5). Bytes live in S3; the browser
 * never sees credentials — it loads/downloads through the orchestrator proxy.
 * Images preview inline; text/code gets a short fetched preview; anything else
 * is just a download link.
 */
function ArtifactCard({
  uri,
  name,
  contentType,
}: {
  uri: string;
  name?: string;
  contentType?: string;
}) {
  // Token in the query string: `<img>`/`<a>` can't send an Authorization header.
  const url = apiUrl(`/api/artifacts?uri=${encodeURIComponent(uri)}`);
  const downloadUrl = apiUrl(`/api/artifacts?uri=${encodeURIComponent(uri)}&download=1`);
  const isImage = contentType?.startsWith("image/") ?? false;
  const isText =
    !contentType || contentType.startsWith("text/") || /json|xml|yaml|javascript/.test(contentType);
  const [preview, setPreview] = useState<string>();

  useEffect(() => {
    if (!isText || isImage) return;
    api(`/api/artifacts?uri=${encodeURIComponent(uri)}`)
      .then((r) => r.text())
      .then((t) => setPreview(t.slice(0, 2000)))
      .catch(() => {});
  }, [uri, isText, isImage]);

  return (
    <MsgRow icon="attachment" variant="tool" label="ARTIFACT">
      <div className="artifact">
        <div className="artifact-head">
          📎 <span className="aname">{name ?? "artifact"}</span>
          {contentType && <span className="muted"> ({contentType})</span>}
        </div>
        {isImage && <img className="artifact-img" src={url} alt={name ?? "artifact"} />}
        {!isImage && preview !== undefined && <pre className="artifact-preview">{preview}</pre>}
        <a className="artifact-dl" href={downloadUrl}>
          ⤓ Download
        </a>
      </div>
    </MsgRow>
  );
}

/** A transcript row: round avatar on the left, label-caps header + body on the right. */
function MsgRow({
  icon,
  variant,
  label,
  children,
}: {
  icon: string;
  variant: "user" | "thinking" | "tool" | "agent";
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="msg">
      <div className={`avatar ${variant}`}>
        <Icon name={icon} className="sm" />
      </div>
      <div className="msg-body">
        <div className="msg-head">
          <span className={`label-caps role ${variant}`}>{label}</span>
        </div>
        {children}
      </div>
    </div>
  );
}

/** A collapsible tool-call card (matches the mockup's accordion). */
function ToolRow({ entry }: { entry: Extract<TraceEntry, { kind: "tool" }> }) {
  const [open, setOpen] = useState(false);
  const hasResult = entry.result !== undefined;
  return (
    <MsgRow icon="build" variant="tool" label="TOOL CALL">
      <div className={`tool-card ${open ? "open" : ""}`}>
        <div className="tool-card-head" onClick={() => hasResult && setOpen((v) => !v)}>
          <div className="left">
            <Icon
              name={entry.isError ? "error" : "check_circle"}
              className={`sm fill ${entry.isError ? "err" : "ok"}`}
            />
            <span className="name">
              {entry.toolName}({JSON.stringify(entry.args ?? {})})
            </span>
          </div>
          {hasResult && <Icon name="chevron_right" className="sm chev" />}
        </div>
        {open && hasResult && (
          <pre className={`tool-result ${entry.isError ? "err" : ""}`}>
            {typeof entry.result === "string" ? entry.result : JSON.stringify(entry.result, null, 2)}
          </pre>
        )}
      </div>
    </MsgRow>
  );
}

function TraceRow({ entry, agentName }: { entry: TraceEntry; agentName?: string }) {
  if (entry.kind === "user") {
    return (
      <MsgRow icon="person" variant="user" label="OPERATOR">
        <div className="bubble">{entry.text}</div>
      </MsgRow>
    );
  }

  if (entry.kind === "artifact") {
    return <ArtifactCard uri={entry.uri} name={entry.name} contentType={entry.contentType} />;
  }

  if (entry.kind === "tool") {
    return <ToolRow entry={entry} />;
  }

  // assistant: thinking and answer text become separate rows, each with its own
  // avatar — matching the mockup. Tool calls are their own rows, so skip them.
  const thinking = entry.content
    .filter((c): c is Extract<ContentBlock, { type: "thinking" }> => c.type === "thinking")
    .map((c) => c.thinking)
    .join("\n");
  const text = entry.content
    .filter((c): c is Extract<ContentBlock, { type: "text" }> => c.type === "text")
    .map((c) => c.text)
    .join("\n");

  return (
    <>
      {thinking && (
        <MsgRow icon="psychology" variant="thinking" label="THINKING">
          <div className="think-block">{thinking}</div>
        </MsgRow>
      )}
      {text && (
        <MsgRow
          icon="precision_manufacturing"
          variant="agent"
          label={agentName ?? "AGENT"}
        >
          <div className="bubble agent">{text}</div>
        </MsgRow>
      )}
    </>
  );
}
