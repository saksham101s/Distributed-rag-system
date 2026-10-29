// Persists the Ask conversation in localStorage so it survives a reload.
import type { Source, Telemetry } from "./api";

export interface Turn {
  id: number;
  query: string;
  answer: string;
  sources: Source[];
  telemetry: Telemetry | null;
  cached: boolean;
  provider?: string;
  totalMs?: number;
  phase: "retrieving" | "generating" | "done" | "stopped" | "error";
  error?: string;
  resets: number;
}

export const HISTORY_KEY = "rag.chat.v1";
export const MAX_TURNS = 30;

/** Loads saved turns. Turns that were still streaming when the page closed come back as "stopped". */
export function loadTurns(): Turn[] {
  try {
    const raw = localStorage.getItem(HISTORY_KEY);
    const parsed: unknown = raw ? JSON.parse(raw) : [];
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter((t): t is Turn => typeof t?.id === "number" && typeof t?.query === "string")
      .map((t) => (t.phase === "retrieving" || t.phase === "generating" ? { ...t, phase: "stopped" } : t));
  } catch {
    return [];
  }
}

/** Saves the newest MAX_TURNS turns; if storage is full, keeps dropping the oldest half until it fits. */
export function saveTurns(turns: Turn[]) {
  let keep = turns.slice(-MAX_TURNS);
  while (true) {
    try {
      if (keep.length) localStorage.setItem(HISTORY_KEY, JSON.stringify(keep));
      else localStorage.removeItem(HISTORY_KEY);
      return;
    } catch {
      if (keep.length <= 1) return;
      keep = keep.slice(Math.ceil(keep.length / 2));
    }
  }
}
