import { FormEvent, KeyboardEvent, useEffect, useRef, useState } from "react";
import { streamQuery } from "../lib/api";
import { Turn, loadTurns, saveTurns } from "../lib/chatHistory";
import { AnswerText, SourceCard, TelemetryStrip } from "./AnswerParts";
import { Icon } from "./Icon";
import { ErrorBanner } from "./ui";

const SUGGESTIONS = [
  "How does consistent hashing place documents on shards?",
  "What happens to queries when a shard goes down?",
  "How does scatter-gather merge results?",
];

export function ChatView({ hasDocuments, onGoToDocuments }: { hasDocuments: boolean | null; onGoToDocuments: () => void }) {
  const [turns, setTurns] = useState<Turn[]>(loadTurns);
  const [draft, setDraft] = useState("");
  const [topK, setTopK] = useState(5);
  const [highlight, setHighlight] = useState<{ turn: number; source: number } | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const nextId = useRef(turns.reduce((m, t) => Math.max(m, t.id), 0) + 1);
  const endRef = useRef<HTMLDivElement>(null);
  const busy = turns.some((t) => t.phase === "retrieving" || t.phase === "generating");

  useEffect(() => {
    endRef.current?.scrollIntoView({ block: "end", behavior: "smooth" });
  }, [turns.length, turns[turns.length - 1]?.phase]);

  useEffect(() => () => abortRef.current?.abort(), []);

  // Save when a turn starts or finishes, not on every streamed token
  useEffect(() => {
    saveTurns(turns);
  }, [turns.length, busy]);

  function clearHistory() {
    setTurns([]);
    setHighlight(null);
  }

  const update = (id: number, patch: (t: Turn) => Partial<Turn>) =>
    setTurns((prev) => prev.map((t) => (t.id === id ? { ...t, ...patch(t) } : t)));

  async function ask(query: string) {
    const q = query.trim();
    if (!q || busy) return;
    const id = nextId.current++;
    setDraft("");
    setTurns((prev) => [
      ...prev,
      { id, query: q, answer: "", sources: [], telemetry: null, cached: false, phase: "retrieving", resets: 0 },
    ]);
    const controller = new AbortController();
    abortRef.current = controller;

    await streamQuery(
      q,
      topK,
      {
        onRetrieval: ({ sources, telemetry, cached }) => update(id, () => ({ sources, telemetry, cached, phase: "generating" })),
        onToken: (text) => update(id, (t) => ({ answer: t.answer + text })),
        onReset: () => update(id, (t) => ({ answer: "", resets: t.resets + 1 })),
        onDone: ({ provider_used, latency_ms, cached }) =>
          update(id, () => ({ provider: provider_used, totalMs: latency_ms, cached, phase: "done" })),
        onError: (message) => update(id, () => ({ phase: "error", error: message })),
      },
      controller.signal,
    );
    if (controller.signal.aborted) update(id, (t) => (t.phase === "done" ? {} : { phase: "stopped" }));
  }

  function onSubmit(e: FormEvent) {
    e.preventDefault();
    void ask(draft);
  }

  function onKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault();
      void ask(draft);
    }
  }

  function cite(turnId: number, n: number) {
    setHighlight({ turn: turnId, source: n });
    document.getElementById(`source-${turnId}-${n}`)?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }

  return (
    <div className="chat">
      <div className="chat-scroll">
        {turns.length === 0 ? (
          <div className="chat-empty">
            <h2>Ask the cluster</h2>
            <p>
              Your question is embedded once, searched on every healthy shard in parallel, and the merged top chunks are sent to the
              LLM. Answers stream in as they're generated.
            </p>
            {hasDocuments === false ? (
              <p className="callout">
                No documents yet.{" "}
                <button type="button" className="link-button" onClick={onGoToDocuments}>
                  Add some in Documents
                </button>{" "}
                or run <code>python scripts/seed_documents.py</code>.
              </p>
            ) : (
              <div className="suggestions">
                {SUGGESTIONS.map((s) => (
                  <button key={s} type="button" className="chip" onClick={() => void ask(s)}>
                    {s}
                  </button>
                ))}
              </div>
            )}
          </div>
        ) : (
          <>
          <div className="chat-toolbar">
            <span className="muted small">{turns.length === 1 ? "1 question" : `${turns.length} questions`}
              <span className="col-secondary"> · saved in this browser</span>
            </span>
            <button type="button" className="link-button small" onClick={clearHistory} disabled={busy}>
              Clear conversation
            </button>
          </div>
          <ol className="turns">
            {turns.map((t) => (
              <li key={t.id} className="turn">
                <div className="question">{t.query}</div>
                <article className="answer" aria-busy={t.phase === "retrieving" || t.phase === "generating"}>
                  {t.phase === "retrieving" && <p className="muted pulse">Searching shards…</p>}
                  {t.resets > 0 && t.phase !== "error" && (
                    <p className="muted small">The model stopped part-way, so the answer restarted with the fallback generator.</p>
                  )}
                  {t.answer && (
                    <div aria-live={t.phase === "generating" ? "polite" : undefined}>
                      <AnswerText text={t.answer} onCite={(n) => cite(t.id, n)} />
                      {t.phase === "generating" && <span className="caret" aria-hidden />}
                    </div>
                  )}
                  {t.phase === "stopped" && <p className="muted small">Stopped.</p>}
                  {t.phase === "error" && <ErrorBanner message={t.error ?? "Query failed"} />}

                  {t.telemetry && (
                    <TelemetryStrip telemetry={t.telemetry} totalMs={t.totalMs} provider={t.provider} cached={t.cached} />
                  )}
                  {t.sources.length > 0 && (
                    <div className="sources">
                      <h3>Sources</h3>
                      <ol className="source-list">
                        {t.sources.map((s, i) => (
                          <SourceCard
                            key={s.chunk_id}
                            anchorId={`source-${t.id}-${i + 1}`}
                            source={s}
                            rank={i + 1}
                            highlighted={highlight?.turn === t.id && highlight.source === i + 1}
                          />
                        ))}
                      </ol>
                    </div>
                  )}
                  {t.phase !== "retrieving" && t.telemetry && t.sources.length === 0 && (
                    <p className="muted small">No chunks matched. The answer above has no retrieved context.</p>
                  )}
                </article>
              </li>
            ))}
          </ol>
          </>
        )}
        <div ref={endRef} />
      </div>

      <form className="composer" onSubmit={onSubmit}>
        <label htmlFor="question" className="visually-hidden">
          Question
        </label>
        <textarea
          id="question"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={onKeyDown}
          placeholder="Ask a question about your documents…"
          rows={2}
        />
        <div className="composer-actions">
          <label className="inline-field">
            <span>Top-k</span>
            <select value={topK} onChange={(e) => setTopK(Number(e.target.value))}>
              {[3, 5, 8, 10, 20].map((k) => (
                <option key={k} value={k}>
                  {k}
                </option>
              ))}
            </select>
          </label>
          {busy ? (
            <button type="button" className="button secondary" onClick={() => abortRef.current?.abort()}>
              <Icon name="stop" /> Stop
            </button>
          ) : (
            <button type="submit" className="button primary" disabled={!draft.trim()}>
              <Icon name="send" /> Ask
            </button>
          )}
        </div>
      </form>
    </div>
  );
}
