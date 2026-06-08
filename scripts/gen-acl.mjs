#!/usr/bin/env node
// Regenerate redis/users.acl from passwords in the environment (or a .env file),
// so secrets live in one place. Run after changing any ORCHESTRA_*_PASSWORD /
// AGENT_*_PASSWORD value:  node scripts/gen-acl.mjs
//
// The ACL shape mirrors the hand-written file: role users (broad) + per-agent users
// namespaced to their own control/presence/dedup keys (Phase 6, WS1).
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");

// Load .env (if present) without overriding already-set env vars.
const envFile = join(root, ".env");
if (existsSync(envFile)) {
  for (const line of readFileSync(envFile, "utf8").split("\n")) {
    const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/);
    if (m && process.env[m[1]] === undefined) process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
  }
}

const p = (name, fallback) => process.env[name] ?? fallback;

// Per-agent ACL: read any presence (discovery), write only own; subscribe own
// control stream; own dedupe/position; shared obs/req/reply rails.
const agent = (id, pass) =>
  `user ${id} on >${pass} +@all -@dangerous -@admin resetchannels ` +
  `&presence:events &sessions:events ` +
  `%R~presence:agent:* %RW~presence:agent:${id} ` +
  `%R~cstream:${id} ~cpos:${id} ~dedup:${id}:* ~taskctx:${id}:* ~obs:* ~req:* ~reply:*`;

const lines = [
  `user default on >${p("ORCHESTRA_ADMIN_PASSWORD", "orchestra-admin")} ~* &* +@all`,
  `user dev on >${p("ORCHESTRA_DEV_PASSWORD", "orchestra-dev")} ~* &* +@all -@dangerous`,
  `user orchestrator on >${p("ORCHESTRA_ORCH_PASSWORD", "orchestra-orch")} ~* &* +@all -@dangerous`,
  `user recorder on >${p("ORCHESTRA_REC_PASSWORD", "orchestra-rec")} ~* &* +@all -@dangerous`,
  agent("agent-researcher", p("AGENT_RESEARCHER_PASSWORD", "agent-researcher-pass")),
  agent("agent-writer", p("AGENT_WRITER_PASSWORD", "agent-writer-pass")),
  agent("agent-producer", p("AGENT_PRODUCER_PASSWORD", "agent-producer-pass")),
  agent("agent-reader", p("AGENT_READER_PASSWORD", "agent-reader-pass")),
  agent("agent-echo", p("AGENT_ECHO_PASSWORD", "agent-echo-pass")),
];

const out = join(root, "redis", "users.acl");
writeFileSync(out, lines.join("\n") + "\n");
console.log(`wrote ${out} (${lines.length} users)`);
console.log("apply to a running Redis with: redis-cli -a <admin-pass> ACL LOAD  (or restart Redis)");
