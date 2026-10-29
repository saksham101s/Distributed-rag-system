import { useState } from "react";
import { Source, Telemetry } from "../lib/api";
import { formatMs } from "../lib/format";
import { Icon } from "./Icon";
import { Pill, ShardBadge } from "./ui";

const CITATION = /\[Source (\d+)[^\]]*\]/g;

/** Answer text with "[Source N …]" citations turned into buttons that highlight source card N. */
export function AnswerText({ text, onCite }: { text: string; onCite: (index: number) => void }) {
  const parts: (string | { n: number; raw: string })[] = [];
  let last = 0;
  for (const m of text.matchAll(CITATION)) {
    parts.push(text.slice(last, m.index));
    parts.push({ n: Number(m[1]), raw: m[0] });
    last = (m.index ?? 0) + m[0].length;
  }
  parts.push(text.slice(last));
  return (
    <div className="answer-text">
      {parts.map((p, i) =>
        typeof p === "string" ? (
          p
        ) : (
          <button key={i} type="button" className="citation" onClick={() => onCite(p.n)} title={p.raw}>
            {p.n}
          </button>
        ),
      )}
    </div>
  );
}

export function SourceCard({ source, rank, highlighted, anchorId }: { source: Source; rank: number; highlighted: boolean; anchorId?: string }) {
  const [open, setOpen] = useState(false);
  const filename = typeof source.metadata?.filename === "string" ? source.metadata.filename : null;
  const pct = Math.max(0, Math.min(1, source.score)) * 100;
  return (
    <li id={anchorId} className={`source-card${highlighted ? " highlighted" : ""}`}>
      <div className="source-head">
        <span className="source-rank" aria-label={`Source ${rank}`}>
          {rank}
        </span>
        <span className="source-doc" title={source.doc_id}>
          {filename ?? source.doc_id}
        </span>
        <ShardBadge shardId={source.shard_id} title="Shard whose copy was used" />
        {source.replica_shards && source.replica_shards.length > 0 && (
          <span className="replica-note" title={`Also returned by ${source.replica_shards.join(", ")}; duplicates were merged`}>
            also on {source.replica_shards.join(", ")}
          </span>
        )}
        <span className="score" title={`Cosine similarity ${source.score.toFixed(4)}`}>
          <span className="score-bar" aria-hidden>
            <span style={{ width: `${pct}%` }} />
          </span>
          {source.score.toFixed(3)}
        </span>
      </div>
      <p className={`source-text${open ? " open" : ""}`}>{source.text}</p>
      {source.text.length > 220 && (
        <button type="button" className="link-button" onClick={() => setOpen((o) => !o)} aria-expanded={open}>
          {open ? "Show less" : "Show full chunk"}
        </button>
      )}
    </li>
  );
}

/** Per-shard latency bars for one query, plus the merge summary. */
export function TelemetryStrip({
  telemetry,
  totalMs,
  provider,
  cached,
}: {
  telemetry: Telemetry;
  totalMs?: number;
  provider?: string;
  cached: boolean;
}) {
  const metrics = Object.entries(telemetry.shard_metrics).sort(([a], [b]) => a.localeCompare(b));
  const maxLatency = Math.max(1, ...metrics.map(([, m]) => m.latency_ms));
  const outOfRing = Math.max(0, (telemetry.shards_registered ?? telemetry.shards_queried) - telemetry.shards_queried);
  return (
    <details className="telemetry">
      <summary>
        <span className="telemetry-summary">
          {cached ? (
            <Pill tone="info">cached</Pill>
          ) : telemetry.degraded ? (
            <Pill tone="warn" title={telemetry.errors.join("\n")}>
              degraded
            </Pill>
          ) : outOfRing > 0 ? (
            <Pill tone="warn" title="These shards were down when the query ran; replicas on other shards answered for them">
              {outOfRing} out of ring
            </Pill>
          ) : (
            <Pill tone="ok">all shards</Pill>
          )}
          <span>
            {telemetry.shards_successful}/{telemetry.shards_queried} shards
          </span>
          <span>
            {telemetry.candidates_retrieved} hits → {telemetry.unique_results} unique
          </span>
          <span>scatter {formatMs(telemetry.total_scatter_latency_ms)}</span>
          {totalMs !== undefined && <span>total {formatMs(totalMs)}</span>}
          {provider && <span className="muted">{provider}</span>}
        </span>
      </summary>
      <table className="shard-latency">
        <caption className="visually-hidden">Per-shard search latency</caption>
        <thead>
          <tr>
            <th scope="col">Shard</th>
            <th scope="col">Latency</th>
            <th scope="col">Hits</th>
          </tr>
        </thead>
        <tbody>
          {metrics.map(([shardId, m]) => (
            <tr key={shardId} className={m.status === "failed" ? "failed" : undefined}>
              <td>
                <ShardBadge shardId={shardId} />
              </td>
              <td className="latency-cell">
                <span className="latency-bar" aria-hidden>
                  <span style={{ width: `${(m.latency_ms / maxLatency) * 100}%` }} />
                </span>
                {m.status === "failed" ? (
                  <span className="error-text" title={m.error}>
                    <Icon name="down" size={14} /> {m.error === "Timeout" ? "timed out" : "failed"} after {formatMs(m.latency_ms)}
                  </span>
                ) : (
                  formatMs(m.latency_ms)
                )}
              </td>
              <td>{m.items_retrieved}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </details>
  );
}
