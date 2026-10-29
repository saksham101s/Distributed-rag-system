import { useEffect, useRef, useState } from "react";
import { API_BASE, ClusterStatus } from "../lib/api";

export type FeedEventType =
  | "node_down"
  | "node_up"
  | "repair_completed"
  | "document_ingested"
  | "document_deleted"
  | "fault_injected";

export interface FeedEvent {
  id: number;
  type: FeedEventType;
  timestamp: number;
  data: Record<string, any>;
}

const FEED_TYPES: FeedEventType[] = [
  "node_down",
  "node_up",
  "repair_completed",
  "document_ingested",
  "document_deleted",
  "fault_injected",
];

// Events that can change the document catalog, so views showing it should refetch
const CATALOG_EVENTS = new Set<FeedEventType>(["document_ingested", "document_deleted", "repair_completed", "fault_injected", "node_up", "node_down"]);

export interface ClusterFeed {
  status: ClusterStatus | null;
  events: FeedEvent[];
  connected: boolean;
  /** Bumps whenever an event may have changed the document catalog. */
  catalogVersion: number;
}

/** Subscribes to GET /api/v1/cluster/events. EventSource reconnects on its own; ids de-duplicate the replayed history. */
export function useClusterEvents(maxEvents = 200): ClusterFeed {
  const [status, setStatus] = useState<ClusterStatus | null>(null);
  const [events, setEvents] = useState<FeedEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const [catalogVersion, setCatalogVersion] = useState(0);
  const seen = useRef(new Set<number>());

  useEffect(() => {
    const source = new EventSource(`${API_BASE}/api/v1/cluster/events`);
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(source.readyState === EventSource.OPEN);

    source.addEventListener("cluster_status", (e) => {
      setStatus(JSON.parse((e as MessageEvent).data));
      setConnected(true);
    });

    for (const type of FEED_TYPES) {
      source.addEventListener(type, (e) => {
        const msg = e as MessageEvent;
        const id = Number(msg.lastEventId);
        if (seen.current.has(id)) return;
        seen.current.add(id);
        const { timestamp, ...data } = JSON.parse(msg.data);
        setEvents((prev) => [{ id, type, timestamp, data }, ...prev].slice(0, maxEvents));
        if (CATALOG_EVENTS.has(type)) setCatalogVersion((v) => v + 1);
      });
    }
    return () => source.close();
  }, [maxEvents]);

  return { status, events, connected, catalogVersion };
}
