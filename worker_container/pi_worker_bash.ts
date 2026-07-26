// Fixed worker-child extension: replaces Pi's builtin `bash` with the private
// Worker Harness job plane.  It deliberately registers no wh_ tools and keeps
// all worker/orchestrator identity outside the child process.

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

function childConfig(): { sessionId: string; jobUrl: string } {
  const sessionId = process.env.WH_PI_SESSION_ID?.trim();
  const jobUrl = process.env.WH_PI_JOB_URL?.replace(/\/+$/, "");
  if (!sessionId || !jobUrl) {
    throw new Error("delegated bash is unavailable: worker job service was not configured");
  }
  return { sessionId, jobUrl };
}

async function postState(state: "working" | "idle", eventType: string): Promise<void> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 2_000);
  try {
    const { sessionId, jobUrl } = childConfig();
    const response = await fetch(`${jobUrl}/v1/sessions/${encodeURIComponent(sessionId)}/state`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      signal: controller.signal,
      body: JSON.stringify({ state, event_type: eventType }),
    });
    // Lifecycle reporting must never break a model turn. The relay persists
    // state reports itself and will retry orchestrator delivery separately.
    if (!response.ok) console.warn(`[pi-worker-bash] lifecycle report failed: ${response.status}`);
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
      const { sessionId, jobUrl } = childConfig();
      const controller = new AbortController();
      const abort = () => controller.abort();
      if (signal?.aborted) abort();
      else signal?.addEventListener("abort", abort, { once: true });
      try {
        const response = await fetch(`${jobUrl}/v1/sessions/${encodeURIComponent(sessionId)}/jobs`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          signal: controller.signal,
          body: JSON.stringify({
            command: params.command,
            timeout: params.timeout,
            cwd: ctx.cwd,
          }),
        });
        const raw = await response.text();
        let result: DelegatedJobResult | undefined;
        try {
          result = JSON.parse(raw) as DelegatedJobResult;
        } catch {
          // Use the raw service response below if it was not JSON.
        }
        if (!response.ok || !result) {
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
