#!/usr/bin/env node
// Regenerate seaweedfs/s3.json (SeaweedFS S3 identities) from the S3 credentials
// in the environment (or a .env file), so signed requests authenticate and
// anonymous access is denied. Run after changing S3_ACCESS_KEY / S3_SECRET_KEY:
//   node scripts/gen-s3-config.mjs
//
// One trusted identity with full access; no "anonymous" identity is listed, so
// once any identity exists SeaweedFS enables IAM and rejects unauthenticated
// requests (mirrors gen-acl.mjs: secrets live in .env, this file is derived).
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
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

const config = {
  identities: [
    {
      name: "orchestra",
      credentials: [
        { accessKey: p("S3_ACCESS_KEY", "orchestra"), secretKey: p("S3_SECRET_KEY", "orchestra") },
      ],
      actions: ["Admin", "Read", "Write", "List", "Tagging"],
    },
  ],
};

const dir = join(root, "seaweedfs");
if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
const out = join(dir, "s3.json");
writeFileSync(out, JSON.stringify(config, null, 2) + "\n");
console.log(`wrote ${out} (identity: orchestra; anonymous access denied)`);
console.log("apply with: docker compose -f <file> --env-file .env up -d seaweedfs");
