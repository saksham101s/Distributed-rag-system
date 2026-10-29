import { useCallback, useEffect, useMemo, useState } from "react";
import { useClusterEvents } from "./hooks/useClusterEvents";
import { api } from "./lib/api";
import { formatCount, plural } from "./lib/format";
import { ChatView } from "./components/ChatView";
import { ClusterView, EventFeed } from "./components/ClusterView";
import { DocumentsView } from "./components/DocumentsView";
import { Icon, IconName } from "./components/Icon";
import { ShardBadge, ShardsContext, StatusDot } from "./components/ui";

type Tab = "ask" | "documents" | "cluster";
const TABS: { id: Tab; label: string; icon: IconName }[] = [
  { id: "ask", label: "Ask", icon: "chat" },
  { id: "documents", label: "Documents", icon: "docs" },
  { id: "cluster", label: "Cluster", icon: "cluster" },
];

function tabFromHash(): Tab {
  const t = window.location.hash.replace(/^#\/?/, "");
  return TABS.some((x) => x.id === t) ? (t as Tab) : "ask";
}

export function App() {
  const [tab, setTab] = useState<Tab>(tabFromHash);
  const [docCount, setDocCount] = useState<number | null>(null);
  const { status, events, connected, catalogVersion } = useClusterEvents();
  const shardIds = useMemo(() => status?.nodes.map((n) => n.shard_id) ?? [], [status]);

  useEffect(() => {
    const onHash = () => setTab(tabFromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  // Keep the document count fresh for the header and the chat empty state
  useEffect(() => {
    api.documents().then((r) => setDocCount(r.total), () => {});
  }, [catalogVersion]);

  const go = useCallback((t: Tab) => {
    window.location.hash = `/${t}`;
  }, []);

  const down = status ? status.total_nodes - status.healthy_nodes : 0;
  const health = !connected ? { tone: "neutral" as const, text: "Reconnecting…" } : !status ? { tone: "neutral" as const, text: "Connecting…" } : down ? { tone: "warn" as const, text: `${down} shard${down > 1 ? "s" : ""} down` } : { tone: "ok" as const, text: "All shards healthy" };

  return (
    <ShardsContext.Provider value={shardIds}>
      <div className="app">
        <header className="topbar">
          <div className="brand">
            <img src="/favicon.svg" alt="" width={22} height={22} />
            <span>Distributed RAG</span>
          </div>
          <nav className="tabs" aria-label="Views">
            {TABS.map((t) => (
              <a key={t.id} href={`#/${t.id}`} className="tab" aria-current={tab === t.id ? "page" : undefined}>
                <Icon name={t.icon} />
                {t.label}
                {t.id === "documents" && docCount !== null && <span className="tab-count">{formatCount(docCount)}</span>}
              </a>
            ))}
          </nav>
          <div className="health" aria-live="polite">
            <StatusDot tone={health.tone} label={health.text} />
            <span>{health.text}</span>
          </div>
        </header>

        <main className={`main view-${tab}`}>
          {/* Kept mounted so the conversation survives tab switches */}
          <div className="ask-layout" hidden={tab !== "ask"}>
              <ChatView hasDocuments={docCount === null ? null : docCount > 0} onGoToDocuments={() => go("documents")} />
              <aside className="side-panel" aria-label="Cluster at a glance">
                <h2>Shards</h2>
                {status ? (
                  <ul className="mini-shards">
                    {status.nodes.map((n) => (
                      <li key={n.shard_id}>
                        <ShardBadge shardId={n.shard_id} muted={!n.healthy} />
                        <StatusDot tone={!n.healthy ? "bad" : n.fault.mode === "slow" ? "warn" : "ok"} label={!n.healthy ? "Down" : n.fault.mode === "slow" ? "Slow" : "Healthy"} />
                        <span className="muted small">{plural(n.vector_count, "vector")}</span>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="muted small">Connecting…</p>
                )}
                <h2>Recent activity</h2>
                <EventFeed events={events} limit={6} compact />
                <button type="button" className="link-button small" onClick={() => go("cluster")}>
                  Open cluster view →
                </button>
              </aside>
          </div>
          {tab === "documents" && <DocumentsView catalogVersion={catalogVersion} onCountChange={setDocCount} />}
          {tab === "cluster" && <ClusterView status={status} events={events} />}
        </main>
      </div>
    </ShardsContext.Provider>
  );
}
