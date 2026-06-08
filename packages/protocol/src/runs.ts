/** Body of a request that kicks off an agent run. */
export interface RunRequest {
  prompt: string;
}

/** Body of the correlated reply once a run completes. */
export interface RunResult {
  text: string;
  stopReason?: string;
}
