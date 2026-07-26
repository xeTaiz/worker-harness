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
  // These are Pi lifecycle signals, rather than terminal heuristics; `idle`
  // therefore means the agent truly settled and can drive sync delegation.
  pi.on("agent_start", async () => {
    await postState("working", "agent-start");
  });
  pi.on("agent_settled", async () => {
    await postState("idle", "agent-settled");
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
