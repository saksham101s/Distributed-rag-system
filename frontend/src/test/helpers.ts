// Shared fakes for component tests: a scripted fetch and an inert EventSource.
import { vi } from "vitest";

export type Route = (url: string, init: RequestInit) => Response | Promise<Response>;

export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

export function sseResponse(events: [string, unknown][]): Response {
  const text = events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join("");
  return new Response(text, { status: 200, headers: { "content-type": "text/event-stream" } });
}

/** Installs a fetch mock that answers by the first route whose key is a substring of the URL. */
export function mockFetch(routes: Record<string, Route>) {
  const fn = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const url = String(input);
    for (const [key, route] of Object.entries(routes)) if (url.includes(key)) return route(url, init);
    return jsonResponse({ detail: `no mock for ${url}` }, 404);
  });
  vi.stubGlobal("fetch", fn);
  return fn;
}

export class FakeEventSource {
  static instances: FakeEventSource[] = [];
  static readonly OPEN = 1;
  readyState = 0;
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  listeners = new Map<string, ((e: MessageEvent) => void)[]>();
  closed = false;
  constructor(public url: string) {
    FakeEventSource.instances.push(this);
  }
  addEventListener(type: string, fn: (e: MessageEvent) => void) {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), fn]);
  }
  emit(type: string, data: unknown, id = "") {
    for (const fn of this.listeners.get(type) ?? []) fn(new MessageEvent(type, { data: JSON.stringify(data), lastEventId: id }));
  }
  close() {
    this.closed = true;
  }
}

export const telemetry = {
  total_scatter_latency_ms: 12,
  shards_queried: 3,
  shards_successful: 3,
  shards_registered: 3,
  candidates_retrieved: 6,
  unique_results: 2,
  degraded: false,
  errors: [],
  shard_metrics: {
    shard_0: { status: "success", latency_ms: 4, items_retrieved: 2 },
    shard_1: { status: "success", latency_ms: 6, items_retrieved: 2 },
    shard_2: { status: "success", latency_ms: 12, items_retrieved: 2 },
  },
};

export const source = {
  chunk_id: "doc_raft_0",
  doc_id: "doc_raft",
  text: "Raft elects a leader and replicates a log.",
  score: 0.91,
  shard_id: "shard_0",
  replica_shards: ["shard_2"],
  metadata: {},
};
