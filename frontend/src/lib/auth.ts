// Optional API key (coordinator API_KEY). Kept in localStorage; a 401 anywhere asks the app to show the sign-in screen.

const KEY = "rag.apiKey";
export const AUTH_REQUIRED_EVENT = "rag:auth-required";

export function getApiKey(): string {
  try {
    return localStorage.getItem(KEY) ?? "";
  } catch {
    return "";
  }
}

export function setApiKey(key: string) {
  try {
    if (key) localStorage.setItem(KEY, key);
    else localStorage.removeItem(KEY);
  } catch {
    // Storage unavailable (private mode): the key just won't survive a reload
  }
}

export function authHeaders(): Record<string, string> {
  const key = getApiKey();
  return key ? { authorization: `Bearer ${key}` } : {};
}

export function notifyAuthRequired() {
  window.dispatchEvent(new Event(AUTH_REQUIRED_EVENT));
}
