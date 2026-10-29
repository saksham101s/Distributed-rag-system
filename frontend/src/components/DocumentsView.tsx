import { DragEvent, FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ApiError, DocumentDetail, DocumentEntry, IngestResult, ReplicationStatus, api } from "../lib/api";
import { breakable, formatBytes, timeAgo } from "../lib/format";
import { Icon } from "./Icon";
import { EmptyState, ErrorBanner, Pill, Section, ShardBadge, Tone } from "./ui";

const ACCEPT = ".txt,.md,.markdown,.rst,.csv,.log,.pdf";

const STATUS_LABEL: Record<ReplicationStatus, { tone: Tone; label: string; help: string }> = {
  ok: { tone: "ok", label: "Replicated", help: "Every ring-assigned shard has the latest version" },
  under_replicated: { tone: "warn", label: "Under-replicated", help: "A target shard is missing this document or has an old version. Repair will copy it." },
  over_replicated: { tone: "info", label: "Extra copy", help: "A shard the ring no longer assigns still has a copy. Repair will trim it." },
};

interface UploadItem {
  key: string;
  name: string;
  size: number;
  state: "uploading" | "done" | "error";
  result?: IngestResult;
  error?: string;
}

function ingestSummary(r: IngestResult): string {
  const failed = Object.keys(r.failed_shards);
  const base = `${r.chunks_created} chunk${r.chunks_created === 1 ? "" : "s"} → ${r.target_shards.join(", ")}`;
  return failed.length ? `${base} (failed on ${failed.join(", ")}, repair will retry)` : base;
}

function UploadPanel({ onIngested }: { onIngested: () => void }) {
  const [items, setItems] = useState<UploadItem[]>([]);
  const [dragging, setDragging] = useState(false);
  const [rf, setRf] = useState<number | "">("");
  const [mode, setMode] = useState<"file" | "text">("file");
  const [docId, setDocId] = useState("");
  const [text, setText] = useState("");
  const [textError, setTextError] = useState<string | null>(null);
  const [textBusy, setTextBusy] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  async function uploadFiles(files: FileList | File[]) {
    const list = Array.from(files);
    const batch = list.map((f, i) => ({ key: `${Date.now()}-${i}-${f.name}`, name: f.name, size: f.size, state: "uploading" as const }));
    setItems((prev) => [...batch, ...prev].slice(0, 20));
    await Promise.all(
      list.map(async (file, i) => {
        const key = batch[i].key;
        try {
          const result = await api.upload(file, rf || undefined);
          setItems((prev) => prev.map((it) => (it.key === key ? { ...it, state: "done", result } : it)));
        } catch (e) {
          const error = e instanceof ApiError ? e.message : String(e);
          setItems((prev) => prev.map((it) => (it.key === key ? { ...it, state: "error", error } : it)));
        }
      }),
    );
    onIngested();
  }

  function onDrop(e: DragEvent) {
    e.preventDefault();
    setDragging(false);
    if (e.dataTransfer.files.length) void uploadFiles(e.dataTransfer.files);
  }

  async function submitText(e: FormEvent) {
    e.preventDefault();
    setTextError(null);
    setTextBusy(true);
    try {
      const result = await api.ingestText(docId.trim(), text, rf || undefined);
      setItems((prev) => [{ key: `${Date.now()}-text`, name: result.doc_id, size: new Blob([text]).size, state: "done" as const, result }, ...prev]);
      setDocId("");
      setText("");
      onIngested();
    } catch (err) {
      setTextError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setTextBusy(false);
    }
  }

  return (
    <Section
      title="Add documents"
      actions={
        <>
          <div className="segmented" role="tablist" aria-label="Input type">
            <button type="button" role="tab" aria-selected={mode === "file"} onClick={() => setMode("file")}>
              Upload files
            </button>
            <button type="button" role="tab" aria-selected={mode === "text"} onClick={() => setMode("text")}>
              Paste text
            </button>
          </div>
          <label className="inline-field">
            <span>Replicas</span>
            <select value={rf} onChange={(e) => setRf(e.target.value ? Number(e.target.value) : "")}>
              <option value="">Default</option>
              {[1, 2, 3].map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </label>
        </>
      }
    >
      {mode === "file" ? (
        <div
          className={`dropzone${dragging ? " dragging" : ""}`}
          onDragOver={(e) => {
            e.preventDefault();
            setDragging(true);
          }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
        >
          <Icon name="upload" size={22} />
          <p>
            Drop <strong>.txt</strong>, <strong>.md</strong> or <strong>.pdf</strong> files here, or{" "}
            <button type="button" className="link-button" onClick={() => inputRef.current?.click()}>
              choose files
            </button>
          </p>
          <p className="muted small">The filename becomes the document id. Re-uploading the same name replaces the old version.</p>
          <input
            ref={inputRef}
            type="file"
            accept={ACCEPT}
            multiple
            hidden
            onChange={(e) => {
              if (e.target.files?.length) void uploadFiles(e.target.files);
              e.target.value = "";
            }}
          />
        </div>
      ) : (
        <form className="text-ingest" onSubmit={submitText}>
          <label className="field">
            <span>Document id</span>
            <input value={docId} onChange={(e) => setDocId(e.target.value)} placeholder="e.g. raft-notes" required maxLength={256} />
          </label>
          <label className="field">
            <span>Text</span>
            <textarea value={text} onChange={(e) => setText(e.target.value)} rows={6} required placeholder="Paste the document text…" />
          </label>
          {textError && <ErrorBanner message={textError} onDismiss={() => setTextError(null)} />}
          <div>
            <button type="submit" className="button primary" disabled={textBusy || !docId.trim() || !text.trim()}>
              <Icon name="plus" /> {textBusy ? "Ingesting…" : "Ingest"}
            </button>
          </div>
        </form>
      )}

      {items.length > 0 && (
        <ul className="upload-list" aria-live="polite">
          {items.map((it) => (
            <li key={it.key} className={`upload-item state-${it.state}`}>
              <span className="upload-name">{it.name}</span>
              <span className="muted small">{formatBytes(it.size)}</span>
              {it.state === "uploading" && <span className="muted small pulse">Uploading…</span>}
              {it.state === "done" && it.result && (
                <span className="small">
                  <Pill tone={it.result.status === "success" ? "ok" : "warn"}>{it.result.status}</Pill> {ingestSummary(it.result)}
                </span>
              )}
              {it.state === "error" && <span className="error-text small">{it.error}</span>}
            </li>
          ))}
        </ul>
      )}
    </Section>
  );
}

function DocumentDrawer({ docId, onClose }: { docId: string; onClose: () => void }) {
  const [doc, setDoc] = useState<DocumentDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const closeRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    let live = true;
    setDoc(null);
    setError(null);
    api.document(docId).then(
      (d) => live && setDoc(d),
      (e) => live && setError(e instanceof ApiError ? e.message : String(e)),
    );
    closeRef.current?.focus();
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => {
      live = false;
      window.removeEventListener("keydown", onKey);
    };
  }, [docId, onClose]);

  return (
    <div className="drawer-backdrop" onClick={onClose}>
      <aside className="drawer" role="dialog" aria-modal="true" aria-label={`Document ${docId}`} onClick={(e) => e.stopPropagation()}>
        <header className="drawer-header">
          <h2 title={docId}>{docId}</h2>
          <button ref={closeRef} type="button" className="icon-button" onClick={onClose} aria-label="Close">
            <Icon name="close" />
          </button>
        </header>
        {error && <ErrorBanner message={error} />}
        {!doc && !error && <p className="muted pulse">Loading…</p>}
        {doc && (
          <div className="drawer-body">
            <dl className="facts">
              <dt>Status</dt>
              <dd>
                <Pill tone={STATUS_LABEL[doc.replication_status].tone}>{STATUS_LABEL[doc.replication_status].label}</Pill>
              </dd>
              <dt>Ingested</dt>
              <dd>{new Date(doc.ingested_at * 1000).toLocaleString()}</dd>
              <dt>Chunks</dt>
              <dd>{doc.chunk_count}</dd>
              <dt>Ring targets</dt>
              <dd className="badge-row">
                {doc.target_shards.map((s) => (
                  <ShardBadge key={s} shardId={s} />
                ))}
              </dd>
              <dt>Held by</dt>
              <dd className="badge-row">
                {doc.replicas.map((r) => (
                  <ShardBadge key={r.shard_id} shardId={r.shard_id} muted={r.stale} title={r.stale ? "Old version" : "Latest version"} />
                ))}
              </dd>
              {Object.entries(doc.metadata)
                .filter(([k]) => k !== "doc_id")
                .map(([k, v]) => (
                  <FactRow key={k} label={k} value={v} />
                ))}
            </dl>
            <h3>Chunks</h3>
            <ol className="chunk-list">
              {doc.chunks.map((c) => (
                <li key={c.chunk_id}>
                  <span className="chunk-index">#{c.chunk_index}</span>
                  <p>{c.text}</p>
                </li>
              ))}
            </ol>
            <p className="muted small">Served by {doc.served_by}.</p>
          </div>
        )}
      </aside>
    </div>
  );
}

function FactRow({ label, value }: { label: string; value: unknown }) {
  const text = label === "size_bytes" && typeof value === "number" ? formatBytes(value) : typeof value === "string" ? value : JSON.stringify(value);
  return (
    <>
      <dt>{label.replace(/_/g, " ")}</dt>
      <dd>{text}</dd>
    </>
  );
}

function DeleteButton({ docId, onDeleted }: { docId: string; onDeleted: () => void }) {
  const [armed, setArmed] = useState(false);
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    if (!armed) return;
    const t = setTimeout(() => setArmed(false), 3000);
    return () => clearTimeout(t);
  }, [armed]);

  async function onClick() {
    if (!armed) return setArmed(true);
    setBusy(true);
    try {
      await api.deleteDocument(docId);
      onDeleted();
    } finally {
      setBusy(false);
      setArmed(false);
    }
  }
  return (
    <button type="button" className={`button small ${armed ? "danger" : "ghost"}`} onClick={onClick} disabled={busy} aria-label={armed ? `Confirm delete ${docId}` : `Delete ${docId}`}>
      <Icon name="trash" size={14} /> <span className={armed ? undefined : "btn-label"}>{armed ? "Confirm" : "Delete"}</span>
    </button>
  );
}

export function DocumentsView({ catalogVersion, onCountChange }: { catalogVersion: number; onCountChange: (n: number) => void }) {
  const [docs, setDocs] = useState<DocumentEntry[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [open, setOpen] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const res = await api.documents();
      setDocs(res.documents);
      onCountChange(res.total);
      setError(res.errors.length ? `Some shards didn't answer: ${res.errors.join("; ")}` : null);
    } catch (e) {
      setError(e instanceof ApiError ? e.message : String(e));
    }
  }, [onCountChange]);

  useEffect(() => {
    void load();
  }, [load, catalogVersion]);

  const shown = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!docs || !q) return docs ?? [];
    return docs.filter((d) => d.doc_id.toLowerCase().includes(q) || String(d.metadata.filename ?? "").toLowerCase().includes(q));
  }, [docs, filter]);

  const counts = useMemo(() => {
    const c = { ok: 0, under_replicated: 0, over_replicated: 0 };
    for (const d of docs ?? []) c[d.replication_status]++;
    return c;
  }, [docs]);

  const closeDrawer = useCallback(() => setOpen(null), []);

  return (
    <div className="page">
      <UploadPanel onIngested={load} />

      <Section
        title={docs ? `Documents (${docs.length})` : "Documents"}
        actions={
          <>
            {counts.under_replicated > 0 && <Pill tone="warn">{counts.under_replicated} under-replicated</Pill>}
            {counts.over_replicated > 0 && <Pill tone="info">{counts.over_replicated} extra copies</Pill>}
            <label className="visually-hidden" htmlFor="doc-filter">
              Filter documents
            </label>
            <input id="doc-filter" className="search" type="search" placeholder="Filter…" value={filter} onChange={(e) => setFilter(e.target.value)} />
          </>
        }
      >
        {error && <ErrorBanner message={error} />}
        {docs === null && !error && <p className="muted pulse">Loading documents…</p>}
        {docs?.length === 0 && (
          <EmptyState title="No documents yet">
            <p>Upload a file above, or seed the samples with <code>python scripts/seed_documents.py</code>.</p>
          </EmptyState>
        )}
        {shown.length > 0 && (
          <div className="table-wrap">
            <table className="doc-table">
              <thead>
                <tr>
                  <th scope="col">Document</th>
                  <th scope="col" className="col-secondary">Chunks</th>
                  <th scope="col">Replicas</th>
                  <th scope="col" className="col-status">Status</th>
                  <th scope="col" className="col-secondary">Ingested</th>
                  <th scope="col">
                    <span className="visually-hidden">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {shown.map((d) => {
                  const st = STATUS_LABEL[d.replication_status];
                  const filename = typeof d.metadata.filename === "string" ? d.metadata.filename : null;
                  return (
                    <tr key={d.doc_id}>
                      <td>
                        <button type="button" className="doc-link" onClick={() => setOpen(d.doc_id)}>
                          {breakable(d.doc_id)}
                        </button>
                        {filename && <div className="muted small">{filename}</div>}
                        {d.replication_status !== "ok" && (
                          <div className="mobile-only">
                            <Pill tone={st.tone}>{st.label}</Pill>
                          </div>
                        )}
                      </td>
                      <td className="num col-secondary">{d.chunk_count}</td>
                      <td>
                        <div className="badge-row">
                          {d.replicas.map((r) => (
                            <ShardBadge key={r.shard_id} shardId={r.shard_id} muted={r.stale || !d.target_shards.includes(r.shard_id)} title={r.stale ? "Old version" : d.target_shards.includes(r.shard_id) ? "Ring target" : "Not a ring target (will be trimmed)"} />
                          ))}
                        </div>
                      </td>
                      <td className="col-status">
                        <Pill tone={st.tone} title={st.help}>
                          {st.label}
                        </Pill>
                      </td>
                      <td className="muted small col-secondary" title={new Date(d.ingested_at * 1000).toLocaleString()}>
                        {timeAgo(d.ingested_at)}
                      </td>
                      <td>
                        <div className="row-actions">
                        <button type="button" className="button small ghost" onClick={() => setOpen(d.doc_id)} aria-label={`View ${d.doc_id}`}>
                          <Icon name="eye" size={14} /> <span className="btn-label">View</span>
                        </button>
                        <DeleteButton docId={d.doc_id} onDeleted={load} />
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        {docs && docs.length > 0 && shown.length === 0 && <p className="muted">No documents match “{filter}”.</p>}
      </Section>

      {open && <DocumentDrawer docId={open} onClose={closeDrawer} />}
    </div>
  );
}
