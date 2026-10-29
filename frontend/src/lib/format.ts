export function formatMs(ms: number): string {
  if (!Number.isFinite(ms)) return "–";
  if (ms < 1) return `${ms.toFixed(2)} ms`;
  if (ms < 1000) return `${Math.round(ms)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

export function formatCount(n: number): string {
  return new Intl.NumberFormat().format(n);
}

/** "just now", "12s ago", "5m ago", "3h ago", else a short date. `epochSeconds` is Unix time in seconds. */
export function timeAgo(epochSeconds: number, now: number = Date.now()): string {
  const diff = Math.max(0, now / 1000 - epochSeconds);
  if (diff < 5) return "just now";
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return new Date(epochSeconds * 1000).toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

export function clockTime(epochSeconds: number): string {
  return new Date(epochSeconds * 1000).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

/** Inserts zero-width break opportunities after _ - . / so long ids wrap at word-ish boundaries. */
export function breakable(id: string): string {
  return id.replace(/([_\-./])/g, "$1\u200b");
}

export function plural(n: number, word: string): string {
  return `${formatCount(n)} ${word}${n === 1 ? "" : "s"}`;
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

/** Stable shard → color slot (1..6) so a shard has the same color in every view. */
export function shardSlot(shardId: string | null | undefined, allShards: string[]): number {
  if (!shardId) return 0;
  const idx = [...allShards].sort().indexOf(shardId);
  return idx === -1 ? 0 : (idx % 6) + 1;
}
