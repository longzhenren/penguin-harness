/**
 * The socket transport's tap (PRFC-0011): an endpoint called over the socket captures
 * `sseTap()` once, at its construction, and hands each event to it in place of writing its SSE
 * line — so a stream crosses as the frames it already are, with no text written and parsed
 * back. Captured once because a stream's events are written from the broadcaster's context,
 * long after the request returned; queued rather than sent because the stream start frame has
 * to go first — serve.ts drains the queue once the answer is framed, and the endpoint's own
 * Response body, left empty, still closes exactly when its stream ends.
 */
import { AsyncLocalStorage } from "node:async_hooks";

/** One event of a stream, as the socket frames it: SSE's `event` / `id` / `data`. */
export interface SseTapFrame {
  /** The event's name; "message" when the SSE write carried none (the browser's EventSource default). */
  event: string;
  /** The `id:` line, or null when the event carried none. */
  eventId: string | null;
  /** The event's data, one line in this server's protocol (single-line JSON). */
  data: string;
}

/** One stream's events: the endpoint fills, serve.ts drains. */
export class SseTap {
  readonly #events: SseTapFrame[] = [];
  #sealed = false;
  #wake: (() => void) | null = null;

  /** The endpoint's write face: one event, in place of its SSE line. */
  event(frame: SseTapFrame): void {
    if (this.#sealed) return;
    this.#events.push(frame);
    this.#wake?.();
  }

  /** No event can arrive any more — the body has closed. Queued events still drain. */
  seal(): void {
    this.#sealed = true;
    this.#wake?.();
  }

  /** The next event, or null once sealed and drained. */
  next(): Promise<SseTapFrame | null> {
    if (this.#events.length > 0) return Promise.resolve(this.#events.shift() as SseTapFrame);
    if (this.#sealed) return Promise.resolve(null);
    return new Promise((resolve) => {
      this.#wake = () => {
        this.#wake = null;
        resolve(this.#events.length > 0 ? (this.#events.shift() as SseTapFrame) : null);
      };
    });
  }
}

const taps = new AsyncLocalStorage<SseTap>();

/** Runs `body` — one call, served by socket/serve.ts — with `tap` as its stream tap. */
export function withSseTap<T>(tap: SseTap, body: () => T): T {
  return taps.run(tap, body);
}

/** The tap of the call being served, for an endpoint to capture once at its construction. */
export function sseTap(): SseTap | null {
  return taps.getStore() ?? null;
}
