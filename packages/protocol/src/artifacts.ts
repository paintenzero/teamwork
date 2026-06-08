/**
 * Artifacts (Phase 5). Large outputs — files, images, datasets — live in S3; the
 * bus carries only a *reference*, never the bytes. `ArtifactRef` is that
 * reference. The `artifact` observability event (events.ts) already carries the
 * same fields so the UI/recorder learn about an artifact without seeing its bytes.
 */
export interface ArtifactRef {
  /** `s3://<bucket>/<sessionId>/<uuid>-<name>` — opaque to everyone but the store. */
  uri: string;
  contentType?: string;
  name?: string;
}
