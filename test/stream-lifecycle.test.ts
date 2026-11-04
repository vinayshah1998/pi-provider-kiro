import {
  type AssistantMessage,
  type AssistantMessageEvent,
  createAssistantMessageEventStream,
} from "@earendil-works/pi-ai";
import { describe, expect, it } from "vitest";
import { trackStreamLifecycle } from "../src/stream-lifecycle.js";

function makeMessage(content: AssistantMessage["content"] = []): AssistantMessage {
  return {
    role: "assistant",
    content,
    api: "kiro-api",
    provider: "kiro",
    model: "claude-sonnet-4-5",
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "stop",
    timestamp: 0,
  };
}

async function collect(stream: AsyncIterable<AssistantMessageEvent>): Promise<AssistantMessageEvent[]> {
  const events: AssistantMessageEvent[] = [];
  for await (const event of stream) events.push(event);
  return events;
}

describe("trackStreamLifecycle", () => {
  it("hides empty attempts and starts once when content is published", async () => {
    const stream = createAssistantMessageEventStream();
    const lifecycle = trackStreamLifecycle(stream);
    stream.push({ type: "start", partial: makeMessage() });
    stream.push({ type: "start", partial: makeMessage() });
    expect(lifecycle.publishedContent).toBe(false);

    const message = makeMessage([{ type: "text", text: "visible" }]);
    stream.push({ type: "text_start", contentIndex: 0, partial: message });
    expect(lifecycle.publishedContent).toBe(true);
    stream.push({ type: "text_delta", contentIndex: 0, delta: "visible", partial: message });
    stream.push({ type: "text_end", contentIndex: 0, content: "visible", partial: message });
    stream.push({ type: "done", reason: "stop", message });

    expect(await collect(stream)).toEqual([
      { type: "start", partial: message },
      { type: "text_start", contentIndex: 0, partial: message },
      { type: "text_delta", contentIndex: 0, delta: "visible", partial: message },
      { type: "text_end", contentIndex: 0, content: "visible", partial: message },
      { type: "done", reason: "stop", message },
    ]);
    expect(await stream.result()).toBe(message);
  });

  it("starts an empty successful stream without marking content as published", async () => {
    const stream = createAssistantMessageEventStream();
    const lifecycle = trackStreamLifecycle(stream);
    const message = makeMessage();
    stream.push({ type: "start", partial: message });
    stream.push({ type: "done", reason: "stop", message });

    expect(await collect(stream)).toEqual([
      { type: "start", partial: message },
      { type: "done", reason: "stop", message },
    ]);
    expect(lifecycle.publishedContent).toBe(false);
  });

  it("forwards an error before content without inventing a start event", async () => {
    const stream = createAssistantMessageEventStream();
    const lifecycle = trackStreamLifecycle(stream);
    const message = makeMessage();
    message.stopReason = "error";
    message.errorMessage = "no credentials";
    stream.push({ type: "start", partial: message });
    stream.push({ type: "error", reason: "error", error: message });

    expect(await collect(stream)).toEqual([{ type: "error", reason: "error", error: message }]);
    expect(lifecycle.publishedContent).toBe(false);
    expect(await stream.result()).toBe(message);
  });

  it.each(["done", "error"] as const)("closes only open blocks before %s", async (terminal) => {
    const stream = createAssistantMessageEventStream();
    trackStreamLifecycle(stream);
    const message = makeMessage([
      { type: "thinking", thinking: "closed thought" },
      { type: "text", text: "partial answer" },
      { type: "thinking", thinking: "unfinished thought" },
    ]);
    stream.push({ type: "thinking_start", contentIndex: 0, partial: message });
    stream.push({ type: "thinking_end", contentIndex: 0, content: "closed thought", partial: message });
    stream.push({ type: "text_start", contentIndex: 1, partial: message });
    stream.push({ type: "thinking_start", contentIndex: 2, partial: message });
    if (terminal === "done") {
      stream.push({ type: "done", reason: "stop", message });
    } else {
      message.stopReason = "error";
      message.errorMessage = "connection dropped";
      stream.push({ type: "error", reason: "error", error: message });
    }

    const events = await collect(stream);
    expect(events.filter((event) => event.type === "text_end" || event.type === "thinking_end")).toEqual([
      { type: "thinking_end", contentIndex: 0, content: "closed thought", partial: message },
      { type: "text_end", contentIndex: 1, content: "partial answer", partial: message },
      { type: "thinking_end", contentIndex: 2, content: "unfinished thought", partial: message },
    ]);
    expect(events.at(-1)?.type).toBe(terminal);
    expect(await stream.result()).toBe(message);
  });

  it("marks tool-only output as published without fabricating text blocks", async () => {
    const stream = createAssistantMessageEventStream();
    const lifecycle = trackStreamLifecycle(stream);
    const toolCall = { type: "toolCall", id: "t1", name: "read", arguments: { path: "example.txt" } } as const;
    const message = makeMessage([toolCall]);
    message.stopReason = "toolUse";
    stream.push({ type: "toolcall_start", contentIndex: 0, partial: message });
    expect(lifecycle.publishedContent).toBe(true);
    stream.push({ type: "toolcall_end", contentIndex: 0, toolCall, partial: message });
    stream.push({ type: "done", reason: "toolUse", message });

    expect(await collect(stream)).toEqual([
      { type: "start", partial: message },
      { type: "toolcall_start", contentIndex: 0, partial: message },
      { type: "toolcall_end", contentIndex: 0, toolCall, partial: message },
      { type: "done", reason: "toolUse", message },
    ]);
  });
});
