// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { AnswerText, TelemetryStrip } from "./AnswerParts";
import { telemetry } from "../test/helpers";
import type { Telemetry } from "../lib/api";

afterEach(cleanup);

describe("TelemetryStrip", () => {
  it("says all shards when every registered shard answered", () => {
    render(<TelemetryStrip telemetry={telemetry as Telemetry} cached={false} />);
    expect(screen.getByText("all shards")).toBeTruthy();
    expect(screen.getByText("3/3 shards")).toBeTruthy();
  });

  it("flags shards that were out of the ring", () => {
    render(<TelemetryStrip telemetry={{ ...(telemetry as Telemetry), shards_queried: 2, shards_successful: 2 }} cached={false} />);
    expect(screen.getByText("1 out of ring")).toBeTruthy();
  });

  it("flags a degraded query", () => {
    const t = { ...(telemetry as Telemetry), shards_successful: 2, degraded: true };
    render(<TelemetryStrip telemetry={t} cached={false} />);
    expect(screen.queryByText("all shards")).toBeNull();
    expect(screen.getByText(/degraded/i)).toBeTruthy();
  });
});

describe("AnswerText", () => {
  it("turns [Source n] markers into citation buttons", () => {
    const onCite = vi.fn();
    render(<AnswerText text="Leaders replicate logs [Source 2]." onCite={onCite} />);
    fireEvent.click(screen.getByRole("button", { name: "2" }));
    expect(onCite).toHaveBeenCalledWith(2);
  });
});
