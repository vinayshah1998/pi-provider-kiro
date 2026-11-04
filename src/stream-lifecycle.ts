import type { AssistantMessageEventStream } from "@earendil-works/pi-ai";

export function trackStreamLifecycle(stream: AssistantMessageEventStream): { readonly publishedContent: boolean } {
  let started = false;
  let publishedContent = false;
  const openBlocks = new Map<number, "text" | "thinking">();
  const push = stream.push.bind(stream);
  stream.push = (event) => {
    // Empty attempts may retry privately; the public stream starts only once.
    if (event.type === "start") return;
    if (event.type !== "done" && event.type !== "error") {
      if (!started && "partial" in event) {
        started = true;
        push({ type: "start", partial: event.partial });
      }
      publishedContent = true;
    } else if (event.type === "done" && !started) {
      started = true;
      push({ type: "start", partial: event.message });
    }
    if (event.type === "text_start") openBlocks.set(event.contentIndex, "text");
    if (event.type === "thinking_start") openBlocks.set(event.contentIndex, "thinking");
    if (event.type === "text_end" || event.type === "thinking_end") openBlocks.delete(event.contentIndex);
    if (event.type === "done" || event.type === "error") {
      const message = event.type === "done" ? event.message : event.error;
      for (const [contentIndex, kind] of openBlocks) {
        const block = message.content[contentIndex];
        const content = block?.type === "text" ? block.text : block?.type === "thinking" ? block.thinking : "";
        push({ type: `${kind}_end`, contentIndex, content, partial: message });
      }
      openBlocks.clear();
    }
    push(event);
  };
  return {
    get publishedContent() {
      return publishedContent;
    },
  };
}
