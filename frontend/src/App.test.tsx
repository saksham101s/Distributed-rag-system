// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { App } from "./App";
import { FakeEventSource, jsonResponse, mockFetch } from "./test/helpers";

const status = {
  total_nodes: 3,
  healthy_nodes: 3,
  replication_factor: 2,
  ring_distribution: {},
  nodes: ["shard_0", "shard_1", "shard_2"].map((id) => ({
    shard_id: id,
    address: "x",
    healthy: true,
    vector_count: 1,
    ring_share_pct: 33,
    fault: { mode: "none", latency_ms: 0 },
  })),
  last_repair: {},
  chaos_enabled: false,
};

function authorized(init: RequestInit) {
  return (init.headers as Record<string, string> | undefined)?.authorization === "Bearer good-key";
}

beforeEach(() => {
  localStorage.clear();
  window.location.hash = "";
  FakeEventSource.instances = [];
  vi.stubGlobal("EventSource", FakeEventSource);
  Element.prototype.scrollIntoView = vi.fn();
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("App", () => {
  it("asks for an API key on 401, then signs in with it", async () => {
    mockFetch({
      "/cluster/status": (_u, init) => (authorized(init) ? jsonResponse(status) : jsonResponse({ detail: "Missing or invalid API key." }, 401)),
      "/documents": (_u, init) => (authorized(init) ? jsonResponse({ total: 0, documents: [], errors: [] }) : jsonResponse({}, 401)),
    });
    render(<App />);
    const input = await screen.findByLabelText("API key");

    fireEvent.change(input, { target: { value: "bad-key" } });
    fireEvent.click(screen.getByRole("button", { name: /sign in/i }));
    expect(await screen.findByText("That key was rejected.")).toBeTruthy();
    expect(localStorage.getItem("rag.apiKey")).toBeNull();

    fireEvent.change(input, { target: { value: "good-key" } });
    fireEvent.click(screen.getByRole("button", { name: /sign in/i }));
    await screen.findByRole("button", { name: /sign out/i });
    expect(localStorage.getItem("rag.apiKey")).toBe("good-key");
    // The event stream reconnects with the key in the query string
    expect(FakeEventSource.instances[FakeEventSource.instances.length - 1]?.url).toContain("api_key=good-key");

    fireEvent.click(screen.getByRole("button", { name: /sign out/i }));
    expect(await screen.findByLabelText("API key")).toBeTruthy();
    expect(localStorage.getItem("rag.apiKey")).toBeNull();
  });

  it("opens and closes the shard panel", async () => {
    mockFetch({ "/documents": () => jsonResponse({ total: 2, documents: [], errors: [] }) });
    render(<App />);
    FakeEventSource.instances[0].emit("cluster_status", status);

    const toggle = screen.getByRole("button", { name: "Shard panel" });
    const panel = document.getElementById("side-panel")!;
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    fireEvent.click(toggle);
    expect(toggle.getAttribute("aria-expanded")).toBe("true");
    expect(panel.className).toContain("open");
    fireEvent.keyDown(window, { key: "Escape" });
    expect(panel.className).not.toContain("open");
    await waitFor(() => expect(screen.getByText("2")).toBeTruthy()); // document count badge
  });
});
