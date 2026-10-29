import { ReactNode, createContext, useContext } from "react";
import { shardSlot } from "../lib/format";

/** All shard ids known to the cluster, so badges can pick a stable color slot. */
export const ShardsContext = createContext<string[]>([]);

export function ShardBadge({ shardId, muted = false, title }: { shardId: string | null | undefined; muted?: boolean; title?: string }) {
  const shards = useContext(ShardsContext);
  return (
    <span className={`shard-badge slot-${shardSlot(shardId, shards)}${muted ? " muted" : ""}`} title={title}>
      <span className="swatch" aria-hidden />
      {shardId ?? "unknown"}
    </span>
  );
}

export type Tone = "ok" | "warn" | "bad" | "info" | "neutral";

export function Pill({ tone, children, title }: { tone: Tone; children: ReactNode; title?: string }) {
  return (
    <span className={`pill tone-${tone}`} title={title}>
      {children}
    </span>
  );
}

export function StatusDot({ tone, label }: { tone: Tone; label: string }) {
  return <span className={`status-dot tone-${tone}`} role="img" aria-label={label} title={label} />;
}

export function ErrorBanner({ message, onDismiss }: { message: string; onDismiss?: () => void }) {
  return (
    <div className="banner tone-bad" role="alert">
      <span>{message}</span>
      {onDismiss && (
        <button type="button" className="link-button" onClick={onDismiss}>
          Dismiss
        </button>
      )}
    </div>
  );
}

export function EmptyState({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="empty-state">
      <h3>{title}</h3>
      {children}
    </div>
  );
}

export function Section({ title, actions, children }: { title: string; actions?: ReactNode; children: ReactNode }) {
  return (
    <section className="panel">
      <header className="panel-header">
        <h2>{title}</h2>
        {actions && <div className="panel-actions">{actions}</div>}
      </header>
      {children}
    </section>
  );
}
