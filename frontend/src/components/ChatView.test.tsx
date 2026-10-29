// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { ChatView } from "./ChatView";
import { HISTORY_KEY } from "../lib/chatHistory";
import { mockFetch, source, sseResponse, telemetry } from "../test/helpers";

function streamOk() {
  return sseResponse([
    ["retrieval", { sources: [source], telemetry, cached: false }],
    ["token", { text: "Raft elects a leader " }],
    ["token", { text: "[1]." }],
    ["done", { provider_used: "mock", model_name: "mock", latency_ms: 40, cached: false }],
  ]);
}

async function ask(question: string) {
  fireEvent.change(screen.getByLabelText("Question"), { target: { value: question } });
  fireEvent.click(screen.getByRole("button", { name: /ask/i }));
}

beforeEach(() => {
  localStorage.clear();
  Element.prototype.scrollIntoView = vi.fn();
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("ChatView", () => {
  it("streams an answer with sources, replicas and telemetry", async () => {
    const fetch = mockFetch({ "/query/stream": () => streamOk() });
    render(<ChatView hasDocuments onGoToDocuments={() => {}} />);
    await ask("How does Raft work?");

    await screen.findByText("Raft elects a leader and replicates a log.");
    await waitFor(() => expect(screen.getByText(/total 40 ms/)).toBeTruthy());
    expect(screen.getByText(/also on/i)).toBeTruthy();
    expect(screen.getByText("all shards")).toBeTruthy();
    expect(JSON.parse(String(fetch.mock.calls[0][1]?.body))).toEqual({ query: "How does Raft work?", top_k: 5 });
  });

  it("shows the server error message", async () => {
    mockFetch({ "/query/stream": () => sseResponse([["error", { message: "All shards failed" }]]) });
    render(<ChatView hasDocuments onGoToDocuments={() => {}} />);
    await ask("anything");
    expect(await screen.findByText("All shards failed")).toBeTruthy();
  });

  it("saves the conversation and restores it after a remount, until cleared", async () => {
    mockFetch({ "/query/stream": () => streamOk() });
    const { unmount } = render(<ChatView hasDocuments onGoToDocuments={() => {}} />);
    await ask("How does Raft work?");
    await waitFor(() => expect(screen.getByText(/total 40 ms/)).toBeTruthy());
    await waitFor(() => expect(JSON.parse(localStorage.getItem(HISTORY_KEY) ?? "[]")[0]?.phase).toBe("done"));
    unmount();

    render(<ChatView hasDocuments onGoToDocuments={() => {}} />);
    expect(screen.getByText("How does Raft work?")).toBeTruthy();
    expect(screen.getByText(/1 question/)).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "Clear conversation" }));
    expect(screen.queryByText("How does Raft work?")).toBeNull();
    expect(localStorage.getItem(HISTORY_KEY)).toBeNull();
  });

  it("restores a turn that was mid-stream as stopped", () => {
    localStorage.setItem(
      HISTORY_KEY,
      JSON.stringify([{ id: 7, query: "Half done?", answer: "Partial", sources: [], telemetry: null, cached: false, phase: "generating", resets: 0 }]),
    );
    render(<ChatView hasDocuments onGoToDocuments={() => {}} />);
    expect(screen.getByText("Half done?")).toBeTruthy();
    expect(screen.getByText("Stopped.")).toBeTruthy();
    expect(screen.getByRole("button", { name: /ask/i })).toBeTruthy(); // not busy
  });

  it("points to Documents when the cluster is empty", () => {
    const go = vi.fn();
    render(<ChatView hasDocuments={false} onGoToDocuments={go} />);
    fireEvent.click(screen.getByRole("button", { name: "Add some in Documents" }));
    expect(go).toHaveBeenCalled();
  });
});
