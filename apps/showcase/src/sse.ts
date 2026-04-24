/**
 * Streaming SSE parser for POST requests.
 *
 * The browser's built-in `EventSource` is GET-only, so it can't be
 * used for NebulaDB's `POST /api/v1/ai/rag` stream. Instead we
 * `fetch()` the body as a `ReadableStream`, decode bytes → text, and
 * split on SSE framing rules:
 *
 *   - Lines ending in `\n`.
 *   - A blank line delimits one event.
 *   - Within an event, `event: name` sets the type (default "message").
 *   - `data: ...` lines are concatenated with newlines.
 *
 * We yield typed frames with the event name + accumulated data. The
 * RAG handler emits three event types — `context`, `answer_delta`,
 * `done`, plus `error` on mid-stream LLM failure.
 */

export interface SseFrame {
  event: string;
  data: string;
}

export async function* sseStream(
  url: string,
  body: unknown,
  signal?: AbortSignal
): AsyncGenerator<SseFrame, void, void> {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json", accept: "text/event-stream" },
    body: JSON.stringify(body),
    signal,
  });
  if (!resp.ok || !resp.body) {
    const txt = await resp.text().catch(() => "");
    throw new Error(`SSE request failed: ${resp.status} ${txt}`);
  }

  // Decode incrementally so a multi-byte UTF-8 character split across
  // chunks survives intact. `stream: true` keeps TextDecoder's state.
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  let curEvent = "message";
  let curData: string[] = [];

  const flush = function* (): Generator<SseFrame> {
    if (curData.length === 0 && curEvent === "message") return;
    yield { event: curEvent, data: curData.join("\n") };
    curEvent = "message";
    curData = [];
  };

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        // End of stream: emit any pending frame (server may have
        // omitted the trailing blank line).
        yield* flush();
        return;
      }
      buf += decoder.decode(value, { stream: true });

      // Process every complete line currently in the buffer. A
      // blank line between frames delimits an event; anything else
      // is a field of the in-progress frame.
      let nl: number;
      while ((nl = buf.indexOf("\n")) !== -1) {
        const line = buf.slice(0, nl).replace(/\r$/, "");
        buf = buf.slice(nl + 1);

        if (line === "") {
          yield* flush();
          continue;
        }
        // Per SSE spec: lines starting with `:` are comments
        // (keep-alives). Ignore.
        if (line.startsWith(":")) continue;

        const colon = line.indexOf(":");
        const field = colon === -1 ? line : line.slice(0, colon);
        let value2 = colon === -1 ? "" : line.slice(colon + 1);
        // SSE convention: strip a single leading space after the colon.
        if (value2.startsWith(" ")) value2 = value2.slice(1);

        if (field === "event") curEvent = value2 || "message";
        else if (field === "data") curData.push(value2);
        // `id` / `retry` we don't care about — discard silently.
      }
    }
  } finally {
    // Release the body reader so Fetch can drop the connection if
    // the caller bails out mid-stream (e.g. component unmount).
    reader.releaseLock();
  }
}
