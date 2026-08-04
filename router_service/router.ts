import { join } from "node:path";
import { ModelRuntime } from "@earendil-works/pi-coding-agent";
import type { Model, ModelThinkingLevel } from "@earendil-works/pi-ai";

type RouteRequest = {
  provider: string;
  model: string;
  thinking_level?: ModelThinkingLevel;
  prompt: string;
};

const HOST = process.env.WH_PI_ROUTER_HOST || "0.0.0.0";
const PORT = Number.parseInt(process.env.WH_PI_ROUTER_PORT || "12900", 10);
const AGENT_DIR = process.env.PI_AGENT_DIR || join(process.env.HOME || "/root", ".pi", "agent");
const MAX_PROMPT_CHARS = 256_000;
const MAX_OUTPUT_TOKENS = Math.max(8, Number.parseInt(process.env.WH_PI_ROUTER_MAX_OUTPUT_TOKENS || "64", 10));

const runtime = await ModelRuntime.create({
  authPath: join(AGENT_DIR, "auth.json"),
  modelsPath: join(AGENT_DIR, "models.json"),
  modelsStorePath: join(AGENT_DIR, "models-store.json"),
  allowModelNetwork: true,
  modelRefreshTimeoutMs: 10_000,
});

let serial = Promise.resolve();

function json(payload: unknown, status = 200): Response {
  return Response.json(payload, { status, headers: { "cache-control": "no-store" } });
}

function modelPayload(model: Model<any>) {
  return {
    provider: model.provider,
    id: model.id,
    name: model.name,
    reasoning: Boolean(model.reasoning),
    context_window: model.contextWindow,
    max_tokens: model.maxTokens,
  };
}

function assistantText(message: Awaited<ReturnType<ModelRuntime["completeSimple"]>>): string {
  return message.content
    .filter((block): block is Extract<(typeof message.content)[number], { type: "text" }> => block.type === "text")
    .map((block) => block.text)
    .join("")
    .trim();
}

async function route(payload: RouteRequest) {
  if (!payload || typeof payload !== "object") throw new Error("request body is required");
  const provider = String(payload.provider || "").trim();
  const modelId = String(payload.model || "").trim();
  const prompt = String(payload.prompt || "");
  const thinkingLevel = String(payload.thinking_level || "off") as ModelThinkingLevel;
  if (!provider || !modelId) throw new Error("provider and model are required");
  if (!prompt || prompt.length > MAX_PROMPT_CHARS) throw new Error("invalid router prompt");
  if (!["off", "minimal", "low", "medium", "high", "xhigh", "max"].includes(thinkingLevel)) {
    throw new Error("invalid thinking level");
  }
  const model = runtime.getModel(provider, modelId);
  if (!model || !runtime.hasConfiguredAuth(provider)) {
    throw new Error(`model unavailable or unauthenticated: ${provider}/${modelId}`);
  }
  const started = performance.now();
  const response = await runtime.completeSimple(
    model,
    {
      systemPrompt: "Return only the requested routing integer. Never add explanation or punctuation.",
      messages: [{ role: "user", content: prompt, timestamp: Date.now() }],
    },
    {
      ...(thinkingLevel === "off" ? {} : { reasoning: thinkingLevel }),
      maxTokens: MAX_OUTPUT_TOKENS,
      maxRetries: 0,
    },
  );
  if (response.stopReason === "error" || response.stopReason === "aborted") {
    throw new Error(response.errorMessage || `router model stopped: ${response.stopReason}`);
  }
  return {
    output: assistantText(response),
    latency_ms: Math.max(0, Math.round(performance.now() - started)),
    provider,
    model: modelId,
    thinking_level: thinkingLevel,
  };
}

Bun.serve({
  hostname: HOST,
  port: PORT,
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/healthz") {
      return json({ status: "healthy", model_error: runtime.getError() || null });
    }
    if (request.method === "GET" && url.pathname === "/v1/models") {
      const models = await runtime.getAvailable();
      return json({ models: models.map(modelPayload) });
    }
    if (request.method === "POST" && url.pathname === "/v1/route") {
      try {
        const payload = await request.json() as RouteRequest;
        const task = serial.then(() => route(payload));
        serial = task.then(() => undefined, () => undefined);
        return json(await task);
      } catch (error) {
        return json({ detail: error instanceof Error ? error.message : String(error) }, 422);
      }
    }
    return json({ detail: "not found" }, 404);
  },
});

console.log(`[wh-pi-router] listening on ${HOST}:${PORT}`);
