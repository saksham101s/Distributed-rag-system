import { useCallback, useEffect, useMemo, useState } from "react";
import { useClusterEvents } from "./hooks/useClusterEvents";
import { api } from "./lib/api";
import { AUTH_REQUIRED_EVENT, getApiKey, setApiKey } from "./lib/auth";
import { formatCount, plural } from "./lib/format";
import { ChatView } from "./components/ChatView";
import { ClusterView, EventFeed } from "./components/ClusterView";
import { DocumentsView } from "./components/DocumentsView";
import { SignIn } from "./components/SignIn";
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
  const [apiKey, setKeyState] = useState(getApiKey);
  const [needsAuth, setNeedsAuth] = useState(false);
  const [panelOpen, setPanelOpen] = useState(false);
  const { status, events, connected, catalogVersion } = useClusterEvents(apiKey);
  const shardIds = useMemo(() => status?.nodes.map((n) => n.shard_id) ?? [], [status]);

  useEffect(() => {
    const onHash = () => setTab(tabFromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  useEffect(() => {
    const onAuth = () => setNeedsAuth(true);
    window.addEventListener(AUTH_REQUIRED_EVENT, onAuth);
    return () => window.removeEventListener(AUTH_REQUIRED_EVENT, onAuth);
  }, []);

  // Keep the document count fresh for the header and the chat empty state
  useEffect(() => {
    api.documents().then((r) => setDocCount(r.total), () => {});
  }, [catalogVersion, apiKey]);

  // The narrow-screen shard panel closes on Escape and when leaving the Ask tab
  useEffect(() => {
    if (!panelOpen) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setPanelOpen(false);
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [panelOpen]);
  useEffect(() => setPanelOpen(false), [tab]);

  const signIn = useCallback((key: string) => {
    setKeyState(key);
    setNeedsAuth(false);
  }, []);

  const signOut = useCallback(() => {
    setApiKey("");
    setKeyState("");
    setNeedsAuth(true);
  }, []);

  const go = useCallback((t: Tab) => {
    window.location.hash = `/${t}`;
  }, []);

  if (needsAuth) return <SignIn onSignedIn={signIn} />;

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
          {apiKey && (
            <button type="button" className="button ghost small" onClick={signOut} title="Forget the API key">
              <Icon name="logout" /> <span className="btn-label">Sign out</span>
            </button>
          )}
        </header>

        <main className={`main view-${tab}`}>
          {/* Kept mounted so the conversation survives tab switches */}
          <div className="ask-layout" hidden={tab !== "ask"}>
              <ChatView hasDocuments={docCount === null ? null : docCount > 0} onGoToDocuments={() => go("documents")} />
              <button
                type="button"
                className="button secondary small panel-toggle"
                aria-expanded={panelOpen}
                aria-controls="side-panel"
                aria-label="Shard panel"
                onClick={() => setPanelOpen((o) => !o)}
              >
                <StatusDot tone={health.tone} label={health.text} />
                Shards
              </button>
              {panelOpen && <div className="panel-backdrop" onClick={() => setPanelOpen(false)} />}
              <aside id="side-panel" className={`side-panel${panelOpen ? " open" : ""}`} aria-label="Cluster at a glance">
                <div className="side-panel-close">
                  <button type="button" className="button ghost small" onClick={() => setPanelOpen(false)} aria-label="Close shard panel">
                    <Icon name="close" />
                  </button>
                </div>
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
