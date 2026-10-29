import { FormEvent, useState } from "react";
import { api, ApiError } from "../lib/api";
import { getApiKey, setApiKey } from "../lib/auth";
import { Icon } from "./Icon";
import { ErrorBanner } from "./ui";

/** Shown when the coordinator answers 401: asks for the API_KEY and checks it before letting the user in. */
export function SignIn({ onSignedIn }: { onSignedIn: (key: string) => void }) {
  const [key, setKey] = useState("");
  const [checking, setChecking] = useState(false);
  const [error, setError] = useState<string | null>(getApiKey() ? "The saved API key was rejected. Enter the current one." : null);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    const candidate = key.trim();
    if (!candidate) return;
    const previous = getApiKey();
    setChecking(true);
    setError(null);
    setApiKey(candidate);
    try {
      await api.status();
      onSignedIn(candidate);
    } catch (err) {
      setApiKey(previous);
      setError(err instanceof ApiError && err.status === 401 ? "That key was rejected." : err instanceof Error ? err.message : String(err));
    } finally {
      setChecking(false);
    }
  }

  return (
    <div className="signin">
      <form className="signin-card" onSubmit={onSubmit}>
        <div className="brand">
          <img src="/favicon.svg" alt="" width={22} height={22} />
          <span>Distributed RAG</span>
        </div>
        <h1>Sign in</h1>
        <p className="muted">
          This coordinator requires an API key (its <code>API_KEY</code> setting). The key is kept in this browser only.
        </p>
        <label className="field">
          <span>API key</span>
          <input type="password" autoComplete="current-password" autoFocus value={key} onChange={(e) => setKey(e.target.value)} />
        </label>
        {error && <ErrorBanner message={error} />}
        <button type="submit" className="button primary" disabled={!key.trim() || checking}>
          <Icon name="key" /> {checking ? "Checking…" : "Sign in"}
        </button>
      </form>
    </div>
  );
}
