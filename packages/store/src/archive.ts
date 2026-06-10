import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  CreateBucketCommand,
} from "@aws-sdk/client-s3";
import type { Readable } from "node:stream";
import { uuidv7, type ArtifactRef } from "@paintenzero/orchestra-protocol";

interface S3Opts {
  endpoint?: string;
  bucket?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  region?: string;
}

/** An S3 client pointed at the env-configured endpoint (any S3-compatible store). */
function makeS3Client(opts: S3Opts): S3Client {
  return new S3Client({
    endpoint: opts.endpoint ?? process.env.S3_ENDPOINT ?? "http://localhost:8333",
    region: opts.region ?? process.env.S3_REGION ?? "us-east-1",
    forcePathStyle: true, // RustFS / MinIO / most S3-compatible stores want path-style
    credentials: {
      accessKeyId: opts.accessKeyId ?? process.env.S3_ACCESS_KEY ?? "orchestra",
      secretAccessKey: opts.secretAccessKey ?? process.env.S3_SECRET_KEY ?? "orchestra",
    },
  });
}

/** Create the bucket if it isn't there; ignore "already exists". */
async function ensureBucket(s3: S3Client, bucket: string): Promise<void> {
  try {
    await s3.send(new CreateBucketCommand({ Bucket: bucket }));
  } catch (err) {
    const name = String((err as { name?: string })?.name ?? err);
    if (!/BucketAlreadyOwnedByYou|BucketAlreadyExists/.test(name)) throw err;
  }
}

/** Parse an `s3://bucket/key` uri into its parts. */
function parseS3Uri(uri: string): { bucket: string; key: string } {
  const m = uri.match(/^s3:\/\/([^/]+)\/(.+)$/);
  if (!m) throw new Error(`not an s3:// uri: ${uri}`);
  return { bucket: m[1], key: m[2] };
}

/**
 * Full raw traces archived to S3 as JSONL — one envelope per
 * line. Postgres keeps a `trace_uri` pointer. The bucket/key layout is stable
 * (`traces/<sessionId>.jsonl`) so re-archiving a growing session just overwrites.
 */
export class TraceArchive {
  private readonly s3: S3Client;
  readonly bucket: string;

  constructor(opts: S3Opts = {}) {
    this.bucket = opts.bucket ?? process.env.S3_BUCKET ?? "orchestra";
    this.s3 = makeS3Client(opts);
  }

  ensureBucket(): Promise<void> {
    return ensureBucket(this.s3, this.bucket);
  }

  /** Write the full JSONL trace for a session; returns its `s3://` uri. */
  async putTrace(sessionId: string, lines: unknown[]): Promise<string> {
    const key = `traces/${sessionId}.jsonl`;
    const body = lines.map((l) => JSON.stringify(l)).join("\n") + "\n";
    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: body,
        ContentType: "application/x-ndjson",
      }),
    );
    return `s3://${this.bucket}/${key}`;
  }

  /** Read a JSONL trace back from its `s3://bucket/key` uri. */
  async getTrace(uri: string): Promise<unknown[]> {
    const { bucket, key } = parseS3Uri(uri);
    const res = await this.s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const text = await res.Body!.transformToString();
    return text
      .split("\n")
      .filter(Boolean)
      .map((l) => JSON.parse(l) as unknown);
  }

  /**
   * Archive a session's canonical context — the pi AgentMessage[] the LLM
   * receives — as one JSON object (`contexts/<sessionId>.json`). Re-archiving a
   * growing session overwrites (the array grows monotonically, last write wins).
   */
  async putContext(sessionId: string, messages: unknown[]): Promise<string> {
    const key = `contexts/${sessionId}.json`;
    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: JSON.stringify(messages),
        ContentType: "application/json",
      }),
    );
    return `s3://${this.bucket}/${key}`;
  }

  /** Read a canonical context back from its `s3://bucket/key` uri. */
  async getContext(uri: string): Promise<unknown[]> {
    const { bucket, key } = parseS3Uri(uri);
    const res = await this.s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    return JSON.parse(await res.Body!.transformToString()) as unknown[];
  }

  close(): void {
    this.s3.destroy();
  }
}

/** A fetched artifact: a stream plus the metadata needed to serve/inline it. */
export interface ArtifactStream {
  body: Readable;
  contentType?: string;
  contentLength?: number;
}

/**
 * Artifact bytes in S3 (Phase 5). Distinct from the trace archive: each artifact
 * is its own object under `<sessionId>/<uuid>-<name>`, and the bus only ever
 * carries the returned `ArtifactRef` — never the bytes. `getArtifact` streams so
 * a large object never has to be buffered (the orchestrator proxies the stream
 * straight to the browser).
 */
export class ArtifactStore {
  private readonly s3: S3Client;
  readonly bucket: string;

  constructor(opts: S3Opts = {}) {
    this.bucket = opts.bucket ?? process.env.S3_BUCKET ?? "orchestra";
    this.s3 = makeS3Client(opts);
  }

  ensureBucket(): Promise<void> {
    return ensureBucket(this.s3, this.bucket);
  }

  /** Store bytes under `<sessionId>/<uuid>-<name>`; return only the reference. */
  async putArtifact(
    data: Uint8Array | Buffer | Readable,
    meta: { sessionId: string; name?: string; contentType?: string },
  ): Promise<ArtifactRef> {
    const safeName = (meta.name ?? "artifact").replace(/[^\w.\-]+/g, "_");
    const key = `${meta.sessionId}/${uuidv7()}-${safeName}`;
    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: data,
        ContentType: meta.contentType,
      }),
    );
    return { uri: `s3://${this.bucket}/${key}`, contentType: meta.contentType, name: meta.name };
  }

  /** Stream an artifact back by uri (do not buffer — callers pipe it onward). */
  async getArtifact(uri: string): Promise<ArtifactStream> {
    const { bucket, key } = parseS3Uri(uri);
    const res = await this.s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    return {
      body: res.Body as unknown as Readable,
      contentType: res.ContentType,
      contentLength: res.ContentLength,
    };
  }

  close(): void {
    this.s3.destroy();
  }
}
