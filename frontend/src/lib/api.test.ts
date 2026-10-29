import { describe, expect, it } from "vitest";
import { createSseParser } from "./api";
import { formatMs, shardSlot, timeAgo } from "./format";

describe("createSseParser", () => {
  it("parses frames split across chunks", () => {
    const parse = createSseParser();
    expect(parse('event: token\ndata: {"text": "he')).toEqual([]);
    expect(parse('llo"}\n\nevent: done\nid: 4\ndata: {}\n\n')).toEqual([
      { event: "token", data: '{"text": "hello"}', id: undefined },
      { event: "done", data: "{}", id: "4" },
    ]);
  });

  it("ignores comments and handles CRLF and multi-line data", () => {
    const parse = createSseParser();
    expect(parse(": keepalive\r\n\r\ndata: a\r\ndata: b\r\n\r\n")).toEqual([{ event: "message", data: "a\nb", id: undefined }]);
  });
});

describe("format", () => {
  it("formats latency", () => {
    expect(formatMs(0.42)).toBe("0.42 ms");
    expect(formatMs(12.6)).toBe("13 ms");
    expect(formatMs(1530)).toBe("1.53 s");
  });

  it("gives relative times", () => {
    const now = 1_000_000_000_000;
    expect(timeAgo(now / 1000 - 2, now)).toBe("just now");
    expect(timeAgo(now / 1000 - 90, now)).toBe("1m ago");
  });

  it("assigns stable shard color slots by sorted id", () => {
    const shards = ["shard_2", "shard_0", "shard_1"];
    expect(shardSlot("shard_0", shards)).toBe(1);
    expect(shardSlot("shard_2", shards)).toBe(3);
    expect(shardSlot("other", shards)).toBe(0);
  });
});
