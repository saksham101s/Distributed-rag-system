import { useContext, useEffect, useState } from "react";
import { ApiError, ClusterStatus, FaultMode, NodeStatus, RepairSummary, api } from "../lib/api";
import { clockTime, formatCount, formatMs, shardSlot, timeAgo } from "../lib/format";
import { FeedEvent } from "../hooks/useClusterEvents";
import { Icon, IconName } from "./Icon";
import { EmptyState, ErrorBanner, Pill, Section, ShardBadge, ShardsContext, StatusDot } from "./ui";

/** Donut of the hash ring: each arc is the share of the 32-bit key space a healthy shard owns as primary. */
function RingChart({ status }: { status: ClusterStatus }) {
  const shards = useContext(ShardsContext);
  const r = 70;
  const circumference = 2 * Math.PI * r;
  let offset = 0;
  const arcs = status.nodes
    .filter((n) => n.healthy && n.ring_share_pct > 0)
    .sort((a, b) => a.shard_id.localeCompare(b.shard_id))
    .map((n) => {
      const len = (n.ring_share_pct / 100) * circumference;
      const arc = { node: n, len, offset };
      offset += len;
      return arc;
    });
  return (
    <figure className="ring">
      <svg viewBox="0 0 180 180" role="img" aria-label="Hash ring key-space share per shard">
        <circle cx="90" cy="90" r={r} className="ring-track" />
        {arcs.map(({ node, len, offset: o }) => (
          <circle
            key={node.shard_id}
            cx="90"
            cy="90"
            r={r}
            className={`ring-arc slot-${shardSlot(node.shard_id, shards)}`}
            strokeDasharray={`${Math.max(0, len - 2)} ${circumference}`}
            strokeDashoffset={-o}
            transform="rotate(-90 90 90)"
          >
            <title>
              {node.shard_id}: {node.ring_share_pct.toFixed(1)}% of keys
            </title>
          </circle>
        ))}
        <text x="90" y="86" className="ring-big">
          {status.healthy_nodes}/{status.total_nodes}
        </text>
        <text x="90" y="106" className="ring-small">
          shards in ring
        </text>
      </svg>
      <figcaption>
        <ul className="ring-legend">
          {status.nodes.map((n) => (
            <li key={n.shard_id}>
              <ShardBadge shardId={n.shard_id} muted={!n.healthy} />
              <span>{n.healthy ? `${n.ring_share_pct.toFixed(1)}%` : "out of ring"}</span>
            </li>
          ))}
        </ul>
      </figcaption>
    </figure>
  );
}

const FAULT_LABEL: Record<FaultMode, string> = { none: "", down: "Unresponsive (injected)", slow: "Slowed (injected)", wipe: "" };

function ShardCard({ node, chaosEnabled, onError }: { node: NodeStatus; chaosEnabled: boolean; onError: (m: string) => void }) {
  const [busy, setBusy] = useState<FaultMode | null>(null);
  const [latency, setLatency] = useState(2000);

  async function inject(mode: FaultMode) {
    setBusy(mode);
    try {
      await api.setFault(node.shard_id, mode, mode === "slow" ? latency : 0);
    } catch (e) {
      onError(e instanceof ApiError ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  }

  const tone = !node.healthy ? "bad" : node.fault.mode === "slow" ? "warn" : "ok";
  const state = !node.healthy ? "Down" : node.fault.mode === "slow" ? "Slow" : "Healthy";
  return (
    <article className={`shard-card${node.healthy ? "" : " is-down"}`}>
      <header>
        <ShardBadge shardId={node.shard_id} />
        <span className="shard-state">
          <StatusDot tone={tone} label={state} /> {state}
        </span>
      </header>
      <dl className="shard-stats">
        <div>
          <dt>Vectors</dt>
          <dd>{formatCount(node.vector_count)}</dd>
        </div>
        <div>
          <dt>Ring share</dt>
          <dd>{node.healthy ? `${node.ring_share_pct.toFixed(1)}%` : "–"}</dd>
        </div>
        <div>
          <dt>Address</dt>
          <dd className="mono">{node.address}</dd>
        </div>
      </dl>
      {node.fault.mode !== "none" && FAULT_LABEL[node.fault.mode] && (
        <p className="fault-note">
          <Icon name="bolt" size={14} /> {FAULT_LABEL[node.fault.mode]}
          {node.fault.mode === "slow" && ` +${formatMs(node.fault.latency_ms)} per call`}
        </p>
      )}
      {chaosEnabled && (
        <div className="chaos-controls" aria-label={`Fault injection for ${node.shard_id}`}>
          {node.fault.mode === "none" ? (
            <>
              <button type="button" className="button small ghost" disabled={!!busy} onClick={() => inject("down")} title="Shard fails every request; heartbeat removes it from the ring">
                <Icon name="pause" size={14} /> Take down
              </button>
              <span className="slow-group">
                <button type="button" className="button small ghost" disabled={!!busy} onClick={() => inject("slow")} title="Adds latency to every request">
                  <Icon name="bolt" size={14} /> Slow
                </button>
                <label className="visually-hidden" htmlFor={`lat-${node.shard_id}`}>
                  Added latency (ms)
                </label>
                <input id={`lat-${node.shard_id}`} type="number" min={0} max={30000} step={250} value={latency} onChange={(e) => setLatency(Number(e.target.value))} />
                <span className="muted small">ms</span>
              </span>
            </>
          ) : (
            <button type="button" className="button small primary" disabled={!!busy} onClick={() => inject("none")}>
              <Icon name="play" size={14} /> Recover
            </button>
          )}
          <button type="button" className="button small ghost" disabled={!!busy} onClick={() => inject("wipe")} title="Empty this shard's memory, like a crash-restart; repair refills it">
            <Icon name="wipe" size={14} /> Wipe
          </button>
        </div>
      )}
    </article>
  );
}

function repairText(s: Partial<RepairSummary>): string {
  const parts = [];
  if (s.copied) parts.push(`copied ${s.copied}`);
  if (s.trimmed) parts.push(`trimmed ${s.trimmed}`);
  if (s.tombstoned) parts.push(`removed ${s.tombstoned} deleted`);
  return parts.length ? parts.join(", ") : "nothing to fix";
}

const EVENT_META: Record<FeedEvent["type"], { icon: IconName; tone: "ok" | "warn" | "bad" | "info" | "neutral" }> = {
  node_down: { icon: "down", tone: "bad" },
  node_up: { icon: "up", tone: "ok" },
  repair_completed: { icon: "repair", tone: "info" },
  document_ingested: { icon: "plus", tone: "neutral" },
  document_deleted: { icon: "trash", tone: "neutral" },
  fault_injected: { icon: "bolt", tone: "warn" },
};

export function describeEvent(e: FeedEvent): string {
  const d = e.data;
  switch (e.type) {
    case "node_down":
      return `${d.shard_id} stopped responding and left the ring`;
    case "node_up":
      return `${d.shard_id} is back in the ring`;
    case "repair_completed":
      return `Repair ${repairText(d)} across ${d.documents_seen} docs in ${formatMs(d.duration_ms)}${d.errors?.length ? ` (${d.errors.length} errors)` : ""}`;
    case "document_ingested": {
      const failed = Object.keys(d.failed_shards ?? {});
      return `Ingested ${d.doc_id} (${d.chunks} chunk${d.chunks === 1 ? "" : "s"}) → ${(d.target_shards ?? []).join(", ")}${failed.length ? `, failed on ${failed.join(", ")}` : ""}`;
    }
    case "document_deleted":
      return `Deleted ${d.doc_id}`;
    case "fault_injected":
      if (d.requested === "wipe") return `Wiped ${d.shard_id}'s memory`;
      if (d.requested === "none") return `Cleared the fault on ${d.shard_id}`;
      if (d.requested === "slow") return `Slowed ${d.shard_id} by ${formatMs(d.latency_ms)} per call`;
      return `Took ${d.shard_id} down`;
  }
}

export function EventFeed({ events, limit, compact = false }: { events: FeedEvent[]; limit?: number; compact?: boolean }) {
  const [, tick] = useState(0);
  useEffect(() => {
    const t = setInterval(() => tick((n) => n + 1), 10_000);
    return () => clearInterval(t);
  }, []);
  const shown = limit ? events.slice(0, limit) : events;
  if (!shown.length) return <p className="muted small">No activity yet. Ingest a document or inject a fault to see events here.</p>;
  return (
    <ol className={`event-feed${compact ? " compact" : ""}`}>
      {shown.map((e) => {
        const meta = EVENT_META[e.type];
        return (
          <li key={e.id} className={`event tone-${meta.tone}`}>
            <span className="event-icon">
              <Icon name={meta.icon} size={14} />
            </span>
            <span className="event-text">{describeEvent(e)}</span>
            <time className="event-time" dateTime={new Date(e.timestamp * 1000).toISOString()} title={clockTime(e.timestamp)}>
              {timeAgo(e.timestamp)}
            </time>
          </li>
        );
      })}
    </ol>
  );
}

export function ClusterView({ status, events }: { status: ClusterStatus | null; events: FeedEvent[] }) {
  const [error, setError] = useState<string | null>(null);
  const [repairing, setRepairing] = useState(false);

  if (!status) {
    return (
      <div className="page">
        <EmptyState title="Waiting for the coordinator">
          <p>Connecting to the live cluster stream…</p>
        </EmptyState>
      </div>
    );
  }

  const totalVectors = status.nodes.reduce((sum, n) => sum + n.vector_count, 0);
  const last = status.last_repair;

  async function repair() {
    setRepairing(true);
    try {
      await api.repair();
    } catch (e) {
      setError(e instanceof ApiError ? e.message : String(e));
    } finally {
      setRepairing(false);
    }
  }

  return (
    <div className="page cluster-page">
      {error && <ErrorBanner message={error} onDismiss={() => setError(null)} />}
      <div className="stat-row">
        <div className="stat">
          <span className="stat-label">Healthy shards</span>
          <span className={`stat-value${status.healthy_nodes < status.total_nodes ? " warn" : ""}`}>
            {status.healthy_nodes}/{status.total_nodes}
          </span>
        </div>
        <div className="stat">
          <span className="stat-label">Vectors stored</span>
          <span className="stat-value">{formatCount(totalVectors)}</span>
          <span className="stat-help">includes replicas</span>
        </div>
        <div className="stat">
          <span className="stat-label">Replication factor</span>
          <span className="stat-value">{status.replication_factor}</span>
        </div>
        <div className="stat">
          <span className="stat-label">Last repair</span>
          <span className="stat-value small">{last.finished_at ? repairText(last) : "not run yet"}</span>
          {last.finished_at && <span className="stat-help">{timeAgo(last.finished_at)}</span>}
        </div>
      </div>

      <div className="cluster-grid">
        <Section title="Hash ring">
          <RingChart status={status} />
        </Section>

        <Section
          title="Shards"
          actions={
            <button type="button" className="button small secondary" onClick={repair} disabled={repairing}>
              <Icon name="repair" size={14} /> {repairing ? "Repairing…" : "Run repair"}
            </button>
          }
        >
          {!status.chaos_enabled && (
            <p className="muted small">
              Failure controls are off. Start the coordinator and shards with <code>CHAOS_ENABLED=true</code> to take shards down from here.
            </p>
          )}
          <div className="shard-grid">
            {status.nodes.map((n) => (
              <ShardCard key={n.shard_id} node={n} chaosEnabled={status.chaos_enabled} onError={setError} />
            ))}
          </div>
          {last.errors && last.errors.length > 0 && (
            <details className="repair-errors">
              <summary>
                <Pill tone="bad">{last.errors.length} repair errors</Pill>
              </summary>
              <ul>
                {last.errors.map((e, i) => (
                  <li key={i} className="mono small">
                    {e}
                  </li>
                ))}
              </ul>
            </details>
          )}
        </Section>
      </div>

      <Section title="Activity">
        <EventFeed events={events} />
      </Section>
    </div>
  );
}
