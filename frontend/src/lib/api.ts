// Typed client for the coordinator REST + SSE API (see docs/ARCHITECTURE.md §3).

export const API_BASE: string = import.meta.env.VITE_API_BASE ?? "";

export type FaultMode = "none" | "down" | "slow" | "wipe";

export interface NodeStatus {
  shard_id: string;
  address: string;
  healthy: boolean;
  vector_count: number;
  ring_share_pct: number;
  fault: { mode: FaultMode; latency_ms: number };
}

export interface RepairSummary {
  copied: number;
  trimmed: number;
  tombstoned: number;
  errors: string[];
  documents_seen: number;
  shards: string[];
  duration_ms: number;
  finished_at: number;
}

export interface ClusterStatus {
  total_nodes: number;
  healthy_nodes: number;
  replication_factor: number;
  ring_distribution: Record<string, number>;
  nodes: NodeStatus[];
  last_repair: Partial<RepairSummary>;
  chaos_enabled: boolean;
}

export interface Source {
  chunk_id: string;
  doc_id: string;
  text: string;
  score: number;
  shard_id: string | null;
  /** Other shards that returned this same chunk (replicas); the merge kept one copy. */
  replica_shards?: string[];
  metadata: Record<string, unknown>;
}

export interface ShardMetric {
  status: "success" | "failed";
  latency_ms: number;
  items_retrieved: number;
  error?: string;
}

export interface Telemetry {
  total_scatter_latency_ms: number;
  shards_queried: number;
  shards_successful: number;
  /** Shards registered with the coordinator; more than shards_queried means some were out of the ring. */
  shards_registered?: number;
  candidates_retrieved: number;
  unique_results: number;
  degraded: boolean;
  errors: string[];
  shard_metrics: Record<string, ShardMetric>;
}

export type ReplicationStatus = "ok" | "under_replicated" | "over_replicated";

export interface DocumentEntry {
  doc_id: string;
  doc_version: number;
  ingested_at: number;
  chunk_count: number;
  replication_factor: number;
  metadata: Record<string, unknown>;
  target_shards: string[];
  replicas: { shard_id: string; doc_version: number; stale: boolean }[];
  replication_status: ReplicationStatus;
}

export interface DocumentDetail extends DocumentEntry {
  chunks: { chunk_id: string; chunk_index: number; text: string }[];
  served_by: string;
}

export interface IngestResult {
  doc_id: string;
  doc_version: number;
  chunks_created: number;
  target_shards: string[];
  failed_shards: Record<string, string>;
  status: "success" | "partial";
  characters?: number | null;
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

function errorMessage(body: unknown, fallback: string): string {
  const detail = (body as { detail?: unknown } | null)?.detail;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) return detail.map((d) => d?.msg ?? JSON.stringify(d)).join("; ");
  if (detail && typeof detail === "object" && "message" in detail) return String((detail as { message: unknown }).message);
  return fallback;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response;
  try {
    res = await fetch(`${API_BASE}${path}`, init);
  } catch {
    throw new ApiError(0, "Can't reach the coordinator. Is it running on port 8000?");
  }
  const body = await res.json().catch(() => null);
  if (!res.ok) throw new ApiError(res.status, errorMessage(body, `${res.status} ${res.statusText}`));
  return body as T;
}

const json = (body: unknown): RequestInit => ({
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify(body),
});

export const api = {
  status: () => request<ClusterStatus>("/api/v1/cluster/status"),
  repair: () => request<RepairSummary>("/api/v1/cluster/repair", { method: "POST" }),
  documents: () => request<{ total: number; documents: DocumentEntry[]; errors: string[] }>("/api/v1/documents?limit=1000"),
  document: (docId: string) => request<DocumentDetail>(`/api/v1/documents/${encodeURIComponent(docId)}`),
  deleteDocument: (docId: string) =>
    request<{ doc_id: string }>(`/api/v1/documents/${encodeURIComponent(docId)}`, { method: "DELETE" }),
  ingestText: (docId: string, text: string, replicationFactor?: number) =>
    request<IngestResult>("/api/v1/documents/ingest", json({ doc_id: docId, text, replication_factor: replicationFactor })),
  upload: (file: File, replicationFactor?: number) => {
    const form = new FormData();
    form.append("file", file);
    if (replicationFactor) form.append("replication_factor", String(replicationFactor));
    return request<IngestResult>("/api/v1/documents/upload", { method: "POST", body: form });
  },
  setFault: (shardId: string, mode: FaultMode, latencyMs = 0) =>
    request<{ mode: FaultMode }>(`/api/v1/chaos/shards/${encodeURIComponent(shardId)}`, json({ mode, latency_ms: latencyMs })),
};

// ---------- Server-Sent Events ----------

export interface SseMessage {
  event: string;
  data: string;
  id?: string;
}

/** Incremental SSE parser: feed it text chunks, it returns the complete messages seen so far. */
export function createSseParser() {
  let buffer = "";
  return (chunk: string): SseMessage[] => {
    buffer += chunk.replace(/\r\n?/g, "\n");
    const frames = buffer.split("\n\n");
    buffer = frames.pop() ?? "";
    const out: SseMessage[] = [];
    for (const frame of frames) {
      let event = "message";
      let id: string | undefined;
      const data: string[] = [];
      for (const line of frame.split("\n")) {
        if (!line || line.startsWith(":")) continue;
        const colon = line.indexOf(":");
        const field = colon === -1 ? line : line.slice(0, colon);
        const value = colon === -1 ? "" : line.slice(colon + 1).replace(/^ /, "");
        if (field === "event") event = value;
        else if (field === "data") data.push(value);
        else if (field === "id") id = value;
      }
      if (data.length) out.push({ event, data: data.join("\n"), id });
    }
    return out;
  };
}

export interface StreamHandlers {
  onRetrieval: (payload: { sources: Source[]; telemetry: Telemetry; cached: boolean }) => void;
  onToken: (text: string) => void;
  onReset: () => void;
  onDone: (payload: { provider_used: string; model_name: string; latency_ms: number; cached: boolean }) => void;
  onError: (message: string) => void;
}

/** POSTs a query to /query/stream and dispatches its SSE events. EventSource can't POST, so this reads the body stream. */
export async function streamQuery(query: string, topK: number, handlers: StreamHandlers, signal: AbortSignal) {
  let res: Response;
  try {
    res = await fetch(`${API_BASE}/api/v1/query/stream`, { ...json({ query, top_k: topK }), signal });
  } catch (e) {
    if (signal.aborted) return;
    handlers.onError("Can't reach the coordinator. Is it running on port 8000?");
    return;
  }
  if (!res.ok || !res.body) {
    const body = await res.json().catch(() => null);
    handlers.onError(errorMessage(body, `${res.status} ${res.statusText}`));
    return;
  }

  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
  const parse = createSseParser();
  let finished = false;
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      for (const msg of parse(value)) {
        const data = JSON.parse(msg.data);
        if (msg.event === "retrieval") handlers.onRetrieval(data);
        else if (msg.event === "token") handlers.onToken(data.text);
        else if (msg.event === "reset") handlers.onReset();
        else if (msg.event === "done") {
          finished = true;
          handlers.onDone(data);
        } else if (msg.event === "error") {
          finished = true;
          handlers.onError(data.message);
        }
      }
    }
  } catch (e) {
    if (signal.aborted) return;
    handlers.onError(e instanceof Error ? e.message : String(e));
    return;
  }
  if (!finished && !signal.aborted) handlers.onError("The stream ended before the answer finished.");
}
