// Fixed worker-child extension: replaces Pi's builtin `bash` with the private
// Worker Harness job plane.  It deliberately registers no wh_ tools and keeps
// all worker/orchestrator identity outside the child process.

import { request as requestOverUnixSocket } from "node:http";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { Type } from "typebox";

type DelegatedJobResult = {
  id: string;
  origin_session_id: string;
  tmux_session: string;
  status: "pending" | "running" | "done" | "failed";
  exit_code: number | null;
  started_at: number;
  finished_at: number;
  output: string;
};

type ChildConfig = { sessionId: string; jobSocket: string };
type UnixHttpResponse = { status: number; text: string };
type BridgeEvent = {
  id: string;
  event_type: string;
  payload: Record<string, unknown>;
  created_at: number;
};
type StreamMessage = {
  role?: string;
  content?: unknown;
  timestamp?: number;
  toolCallId?: string;
  toolName?: string;
  isError?: boolean;
  provider?: string;
  model?: string;
  stopReason?: string;
  errorMessage?: string;
};

const DELTA_FLUSH_MS = 200;
const OUTBOX_RETRY_MS = 2_000;
const MAX_OUTBOX_EVENTS = 2_000;

function childConfig(): ChildConfig {
  const sessionId = process.env.WH_PI_SESSION_ID?.trim();
  const jobSocket = process.env.WH_PI_JOB_SOCKET?.trim();
  if (!sessionId || !jobSocket) {
    throw new Error("delegated bash is unavailable: worker job socket was not configured");
  }
  return { sessionId, jobSocket };
}

/** Send one JSON request without opening any TCP listener or connection. */
function postJson(socketPath: string, path: string, payload: unknown, signal?: AbortSignal): Promise<UnixHttpResponse> {
  const body = JSON.stringify(payload);
  return new Promise((resolve, reject) => {
    let finished = false;
    let request: ReturnType<typeof requestOverUnixSocket> | undefined;
    const cleanup = () => signal?.removeEventListener("abort", abort);
    const resolveOnce = (value: UnixHttpResponse) => {
      if (finished) return;
      finished = true;
      cleanup();
      resolve(value);
    };
    const rejectOnce = (error: Error) => {
      if (finished) return;
      finished = true;
      cleanup();
      reject(error);
    };
    const abort = () => request?.destroy(new Error("delegated job request aborted"));

    request = requestOverUnixSocket(
      {
        socketPath,
        path,
        method: "POST",
        headers: {
          "content-type": "application/json",
          "content-length": String(Buffer.byteLength(body)),
        },
      },
      (response) => {
        let text = "";
        response.setEncoding("utf8");
        response.on("data", (chunk: string) => {
          text += chunk;
        });
        response.once("end", () => resolveOnce({ status: response.statusCode ?? 0, text }));
        response.once("error", rejectOnce);
      },
    );
    request.once("error", rejectOnce);
    if (signal?.aborted) abort();
    else signal?.addEventListener("abort", abort, { once: true });
    request.end(body);
  });
}

async function postState(state: "working" | "idle", eventType: string): Promise<void> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 2_000);
  try {
    const { sessionId, jobSocket } = childConfig();
    const response = await postJson(
      jobSocket,
      `/v1/sessions/${encodeURIComponent(sessionId)}/state`,
      { state, event_type: eventType },
      controller.signal,
    );
    // Lifecycle reporting must never break a model turn. The relay persists
    // state reports itself and will retry orchestrator delivery separately.
    if (response.status < 200 || response.status >= 300) {
      console.warn(`[pi-worker-bash] lifecycle report failed: ${response.status}`);
    }
  } catch (error) {
    console.warn(`[pi-worker-bash] lifecycle report unavailable: ${String(error)}`);
  } finally {
    clearTimeout(timeout);
  }
}

export default function registerWorkerBash(pi: ExtensionAPI) {
  const eventOutbox: BridgeEvent[] = [];
  const pendingDeltas = new Map<string, { messageId: string; contentIndex: number; delta: string }>();
  let flushing = false;
  let outboxTimer: ReturnType<typeof setInterval> | null = null;
  let deltaTimer: ReturnType<typeof setInterval> | null = null;

  function messageId(message: StreamMessage): string {
    return `${message.role ?? "unknown"}:${message.timestamp ?? Date.now()}:${message.toolCallId ?? "message"}`;
  }

  function sanitizeMessage(message: StreamMessage): Record<string, unknown> {
    const blocks = typeof message.content === "string"
      ? [{ type: "text", text: message.content }]
      : Array.isArray(message.content)
        ? message.content.flatMap((raw) => {
            if (!raw || typeof raw !== "object") return [];
            const block = raw as Record<string, unknown>;
            if (block.type === "text") return [{ type: "text", text: String(block.text ?? "") }];
            if (block.type === "image") return [{ type: "image", mimeType: String(block.mimeType ?? "") }];
            if (block.type === "toolCall") return [{
              type: "toolCall", id: String(block.id ?? ""), name: String(block.name ?? ""),
              arguments: block.arguments ?? {},
            }];
            return [];
          })
        : [];
    return {
      role: message.role ?? "unknown",
      timestamp: message.timestamp ?? Date.now(),
      content: blocks,
      ...(message.toolCallId ? { toolCallId: message.toolCallId } : {}),
      ...(message.toolName ? { toolName: message.toolName } : {}),
      ...(message.isError !== undefined ? { isError: message.isError } : {}),
      ...(message.provider ? { provider: message.provider } : {}),
      ...(message.model ? { model: message.model } : {}),
      ...(message.stopReason ? { stopReason: message.stopReason } : {}),
      ...(message.errorMessage ? { errorMessage: message.errorMessage } : {}),
    };
  }

  function queueEvent(eventType: string, payload: Record<string, unknown>, essential = false): void {
    if (eventOutbox.length >= MAX_OUTBOX_EVENTS) {
      if (!essential) return;
      const disposable = eventOutbox.findIndex((event) => event.event_type === "message-delta");
      if (disposable >= 0) eventOutbox.splice(disposable, 1);
      else eventOutbox.shift();
    }
    eventOutbox.push({
      id: crypto.randomUUID(), event_type: eventType, payload,
      created_at: Math.floor(Date.now() / 1000),
    });
    void flushEventOutbox();
  }

  async function flushEventOutbox(): Promise<void> {
    if (flushing || eventOutbox.length === 0) return;
    flushing = true;
    try {
      while (eventOutbox.length > 0) {
        const batch = eventOutbox.slice(0, 100);
        const { sessionId, jobSocket } = childConfig();
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 2_000);
        let response: UnixHttpResponse;
        try {
          response = await postJson(
            jobSocket, `/v1/sessions/${encodeURIComponent(sessionId)}/events`,
            { events: batch }, controller.signal,
          );
        } finally {
          clearTimeout(timeout);
        }
        if (response.status < 200 || response.status >= 300) return;
        eventOutbox.splice(0, batch.length);
      }
    } catch {
      // The relay persists accepted events. Keep unaccepted IDs for safe retry.
    } finally {
      flushing = false;
    }
  }

  function flushPendingDeltas(targetMessageId?: string): void {
    for (const [key, pending] of pendingDeltas) {
      if (targetMessageId && pending.messageId !== targetMessageId) continue;
      pendingDeltas.delete(key);
      if (pending.delta) queueEvent("message-delta", {
        message_id: pending.messageId,
        content_index: pending.contentIndex,
        delta: pending.delta,
      });
    }
  }

  pi.on("message_start", (event) => {
    const message = event.message as StreamMessage;
    queueEvent("message-start", {
      message_id: messageId(message), role: message.role ?? "unknown",
      timestamp: message.timestamp ?? Date.now(),
    });
  });
  pi.on("message_update", (event) => {
    const update = event.assistantMessageEvent;
    if (update.type !== "text_delta") return;
    const id = messageId(event.message as StreamMessage);
    const key = `${id}:${update.contentIndex}`;
    const pending = pendingDeltas.get(key) ?? { messageId: id, contentIndex: update.contentIndex, delta: "" };
    pending.delta += update.delta;
    pendingDeltas.set(key, pending);
  });
  pi.on("message_end", (event) => {
    const message = event.message as StreamMessage;
    const id = messageId(message);
    flushPendingDeltas(id);
    queueEvent("message-end", { message_id: id, message: sanitizeMessage(message) }, true);
  });
  pi.on("tool_execution_start", (event) => {
    queueEvent("tool-start", {
      tool_call_id: event.toolCallId, tool_name: event.toolName, arguments: event.args,
    }, true);
  });
  pi.on("tool_execution_end", (event) => {
    queueEvent("tool-end", {
      tool_call_id: event.toolCallId, tool_name: event.toolName, is_error: event.isError,
    }, true);
  });

  // These are Pi lifecycle signals, rather than terminal heuristics; `idle`
  // therefore means the agent truly settled and can drive sync delegation.
  pi.on("session_start", async () => {
    // Readiness is explicit: a process merely existing in tmux does not prove
    // that Pi has loaded its TUI and can accept a submitted prompt.
    outboxTimer = setInterval(() => void flushEventOutbox(), OUTBOX_RETRY_MS);
    deltaTimer = setInterval(() => flushPendingDeltas(), DELTA_FLUSH_MS);
    await postState("idle", "bridge-ready");
  });
  pi.on("agent_start", async () => {
    await postState("working", "agent-start");
  });
  pi.on("agent_settled", async () => {
    flushPendingDeltas();
    await flushEventOutbox();
    await postState("idle", "agent-settled");
  });
  pi.on("session_shutdown", async () => {
    if (outboxTimer) clearInterval(outboxTimer);
    if (deltaTimer) clearInterval(deltaTimer);
    outboxTimer = null;
    deltaTimer = null;
    flushPendingDeltas();
    await flushEventOutbox();
  });

  pi.registerTool({
    name: "bash",
    label: "bash",
    description:
      "Execute a bash command in the current working directory. The command runs as a Worker Harness job with durable logs.",
    parameters: Type.Object({
      command: Type.String({ description: "Bash command to execute" }),
      timeout: Type.Optional(Type.Number({ description: "Timeout in seconds (optional, no default timeout)" })),
    }),
    executionMode: "sequential",
    async execute(_toolCallId, params, signal, _onUpdate, ctx) {
      const { sessionId, jobSocket } = childConfig();
      const controller = new AbortController();
      const abort = () => controller.abort();
      if (signal?.aborted) abort();
      else signal?.addEventListener("abort", abort, { once: true });
      try {
        const response = await postJson(
          jobSocket,
          `/v1/sessions/${encodeURIComponent(sessionId)}/jobs`,
          {
            command: params.command,
            timeout: params.timeout,
            cwd: ctx.cwd,
          },
          controller.signal,
        );
        const raw = response.text;
        let result: DelegatedJobResult | undefined;
        try {
          result = JSON.parse(raw) as DelegatedJobResult;
        } catch {
          // Use the raw service response below if it was not JSON.
        }
        if (response.status < 200 || response.status >= 300 || !result) {
          throw new Error(`delegated bash job failed to start (${response.status}): ${raw.slice(0, 2000)}`);
        }
        const output = result.output || "(no output)";
        const footer = `\n\n[Worker job: ${result.id}; status: ${result.status}; log: worker harness job logs]`;
        if (result.exit_code !== 0 && result.exit_code !== null) {
          throw new Error(`${output}${footer}\nCommand exited with code ${result.exit_code}`);
        }
        return {
          content: [{ type: "text" as const, text: output + footer }],
          details: {
            job_id: result.id,
            origin_session_id: result.origin_session_id,
            tmux_session: result.tmux_session,
            status: result.status,
            exit_code: result.exit_code,
          },
        };
      } finally {
        signal?.removeEventListener("abort", abort);
      }
    },
  });
}
