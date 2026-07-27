const $ = (selector) => document.querySelector(selector);
const listView = $("#session-list-view");
const detailView = $("#session-detail-view");
const sessionList = $("#session-list");
const emptyState = $("#empty-state");
const transcript = $("#transcript");
const meta = $("#session-meta");
const sessionControls = $("#session-controls");
const modelControl = $("#model-control");
const thinkingControl = $("#thinking-control");
const controlStatus = $("#control-status");
const backButton = $("#back");
const refreshButton = $("#refresh");
const title = $("#page-title");
const subtitle = $("#page-subtitle");
const connectionDot = $("#connection-dot");
const composer = $("#composer");
const messageInput = $("#message");
const sendButton = $("#send");
const composerStatus = $("#composer-status");
const delivery = $("#delivery");
const jumpLatest = $("#jump-latest");
const installButton = $("#install");

const state = {
  sessions: [],
  selected: null,
  cursor: 0,
  source: null,
  timeline: [],
  items: new Map(),
  expanded: new Set(),
  settings: null,
  followLatest: true,
  installPrompt: null,
};

function node(tag, className, text) {
  const element = document.createElement(tag);
  if (className) element.className = className;
  if (text !== undefined) element.textContent = text;
  return element;
}

function setConnection(status) {
  connectionDot.className = `connection-dot ${status === "online" ? "online" : status === "connecting" ? "connecting" : ""}`;
  connectionDot.title = status[0].toUpperCase() + status.slice(1);
}

function relativeTime(timestamp) {
  if (!timestamp) return "unknown";
  const seconds = Math.max(0, Math.floor(Date.now() / 1000 - timestamp));
  if (seconds < 10) return "now";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function sessionLabel(session) {
  return session.name || session.task || session.cwd?.split("/").filter(Boolean).at(-1) || session.id.slice(0, 8);
}

function sessionContext(session) {
  return [session.host, session.cwd].filter(Boolean).join(" · ") || session.tmux_session || session.id;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { "content-type": "application/json", ...(options.headers || {}) },
  });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try { detail = (await response.json()).detail || detail; } catch { /* response is not JSON */ }
    throw new Error(detail);
  }
  return response.status === 204 ? null : response.json();
}

async function loadSessions({ quiet = false } = {}) {
  if (!quiet) setConnection("connecting");
  try {
    state.sessions = await api("/api/v1/pi/sessions");
    state.sessions.sort((a, b) => (b.updated_at || b.created_at) - (a.updated_at || a.created_at));
    renderSessionList();
    if (state.selected) {
      const updated = state.sessions.find((session) => session.id === state.selected.id);
      if (updated) {
        state.selected = updated;
        renderSessionHeader();
      }
    }
    if (!state.selected) setConnection("online");
  } catch (error) {
    setConnection("offline");
    if (!quiet) showListError(error.message);
  }
}

function showListError(message) {
  sessionList.replaceChildren();
  const card = node("div", "empty-state");
  card.append(node("h2", "", "Could not load sessions"), node("p", "", message));
  sessionList.append(card);
}

function renderSessionList() {
  sessionList.replaceChildren();
  const active = state.sessions.filter((session) => ["working", "idle", "starting"].includes(session.state)).length;
  const working = state.sessions.filter((session) => session.state === "working").length;
  $("#summary").replaceChildren(
    summaryPill(state.sessions.length, "total"),
    summaryPill(active, "active"),
    summaryPill(working, "working"),
  );
  emptyState.classList.toggle("hidden", state.sessions.length !== 0);
  for (const session of state.sessions) {
    const card = node("button", "session-card");
    card.type = "button";
    card.addEventListener("click", () => { location.hash = `session/${encodeURIComponent(session.id)}`; });
    const head = node("div", "session-card-head");
    head.append(
      node("span", `state-dot ${session.state}`),
      node("span", "session-name", sessionLabel(session)),
      node("span", "session-state", session.state.replaceAll("_", " ")),
    );
    const context = node("p", "session-context", sessionContext(session));
    const foot = node("div", "session-foot");
    foot.append(node("span", "", session.session_type), node("span", "", relativeTime(session.updated_at || session.created_at)));
    card.append(head, context, foot);
    sessionList.append(card);
  }
}

function summaryPill(value, label) {
  const pill = node("span", "summary-pill");
  const strong = node("strong", "", String(value));
  pill.append(strong, document.createTextNode(` ${label}`));
  return pill;
}

function renderSessionHeader() {
  if (!state.selected) return;
  title.textContent = sessionLabel(state.selected);
  subtitle.textContent = `${state.selected.session_type} · ${state.selected.state.replaceAll("_", " ")}`;
  meta.replaceChildren();
  for (const value of [state.selected.host, state.selected.cwd, state.selected.worker_id && `worker ${state.selected.worker_id.slice(0, 8)}`].filter(Boolean)) {
    meta.append(node("span", "meta-chip", value));
  }
  renderSessionControls();
}

function renderSessionControls() {
  const available = state.selected?.session_type === "interactive" && state.settings;
  sessionControls.classList.toggle("hidden", !available);
  if (!available) return;
  const selectedValue = `${state.settings.provider}::${state.settings.model}`;
  modelControl.replaceChildren();
  for (const model of state.settings.available_models || []) {
    const option = node("option", "", model.name || `${model.provider}/${model.id}`);
    option.value = `${model.provider}::${model.id}`;
    option.selected = option.value === selectedValue;
    modelControl.append(option);
  }
  if (![...modelControl.options].some((option) => option.value === selectedValue) && state.settings.model) {
    const current = node("option", "", `${state.settings.provider}/${state.settings.model}`);
    current.value = selectedValue;
    current.selected = true;
    modelControl.prepend(current);
  }
  thinkingControl.value = state.settings.thinking_level || "off";
}

async function openSession(id) {
  closeStream();
  listView.classList.add("hidden");
  detailView.classList.remove("hidden");
  backButton.classList.remove("hidden");
  refreshButton.classList.add("hidden");
  transcript.replaceChildren(node("div", "empty-transcript", "Loading durable session history…"));
  state.cursor = 0;
  state.timeline = [];
  state.items.clear();
  state.expanded.clear();
  state.settings = null;
  sessionControls.classList.add("hidden");
  controlStatus.textContent = "";
  state.followLatest = true;
  setConnection("connecting");
  try {
    state.selected = state.sessions.find((session) => session.id === id) || await api(`/api/v1/pi/sessions/${encodeURIComponent(id)}`);
    renderSessionHeader();
    await replayEvents(id);
    renderTranscript();
    connectStream(id);
  } catch (error) {
    transcript.replaceChildren(node("div", "empty-transcript", `Unable to open session: ${error.message}`));
    setConnection("offline");
  }
}

async function replayEvents(id) {
  while (true) {
    const events = await api(`/api/v1/pi/sessions/${encodeURIComponent(id)}/events?after=${state.cursor}&limit=500`);
    for (const event of events) applyEvent(event);
    if (events.length < 500) return;
  }
}

function connectStream(id) {
  closeStream();
  const source = new EventSource(`/api/v1/pi/sessions/${encodeURIComponent(id)}/stream?after=${state.cursor}`);
  state.source = source;
  source.addEventListener("open", () => setConnection("online"));
  source.addEventListener("error", () => setConnection("connecting"));
  source.addEventListener("pi-event", (incoming) => {
    try {
      const event = JSON.parse(incoming.data);
      if (event.sequence <= state.cursor) return;
      applyEvent(event);
      renderTranscript();
    } catch (error) {
      console.warn("Invalid Pi event", error);
    }
  });
}

function closeStream() {
  state.source?.close();
  state.source = null;
}

function ensureMessage(id, role = "assistant", timestamp = 0) {
  let item = state.items.get(id);
  if (!item) {
    item = { kind: "message", id, role, timestamp, chunks: new Map(), content: [], done: false };
    state.items.set(id, item);
    state.timeline.push(id);
  }
  return item;
}

function addTimelineItem(id, item) {
  if (state.items.has(id)) return;
  state.items.set(id, item);
  state.timeline.push(id);
}

function applyEvent(event) {
  state.cursor = Math.max(state.cursor, Number(event.sequence) || 0);
  const payload = event.payload || {};
  if (event.event_type === "message-start") {
    ensureMessage(payload.message_id, payload.role, payload.timestamp || event.created_at * 1000);
    return;
  }
  if (event.event_type === "message-delta") {
    const item = ensureMessage(payload.message_id, "assistant", event.created_at * 1000);
    const index = Number(payload.content_index) || 0;
    item.chunks.set(index, (item.chunks.get(index) || "") + String(payload.delta || ""));
    return;
  }
  if (event.event_type === "message-end") {
    const message = payload.message || {};
    const item = ensureMessage(payload.message_id, message.role, message.timestamp);
    item.role = message.role || item.role;
    item.timestamp = message.timestamp || item.timestamp;
    item.content = Array.isArray(message.content) ? message.content : [];
    item.toolName = message.toolName;
    item.provider = message.provider;
    item.model = message.model;
    item.stopReason = message.stopReason;
    item.errorMessage = message.errorMessage;
    item.done = true;
    return;
  }
  if (event.event_type === "session-settings") {
    state.settings = {
      provider: String(payload.provider || ""),
      model: String(payload.model || ""),
      thinking_level: String(payload.thinking_level || "off"),
      available_models: Array.isArray(payload.available_models) ? payload.available_models : [],
    };
    renderSessionControls();
    controlStatus.textContent = "";
    modelControl.disabled = false;
    thinkingControl.disabled = false;
    return;
  }
  if (event.event_type === "control-error") {
    controlStatus.textContent = String(payload.detail || "Control change failed");
    modelControl.disabled = false;
    thinkingControl.disabled = false;
    return;
  }
  if (event.event_type === "tool-start" || event.event_type === "tool-end") {
    const key = `tool:${payload.tool_call_id || event.id}`;
    const existing = state.items.get(key);
    if (existing) {
      existing.done = event.event_type === "tool-end";
      existing.error = Boolean(payload.is_error);
    } else {
      addTimelineItem(key, {
        kind: "tool", id: key, name: payload.tool_name || "tool", args: payload.arguments,
        done: event.event_type === "tool-end", error: Boolean(payload.is_error), timestamp: event.created_at * 1000,
      });
    }
    return;
  }
  addTimelineItem(`event:${event.id}`, {
    kind: "lifecycle", id: `event:${event.id}`, type: event.event_type,
    detail: payload.detail || payload.message || "", timestamp: event.created_at * 1000,
  });
}

function messageText(item) {
  if (item.done) {
    return item.content.map((block) => {
      if (block?.type === "text") return String(block.text || "");
      if (block?.type === "toolCall") return `↳ ${block.name || "tool"}`;
      if (block?.type === "image") return `[image${block.mimeType ? `: ${block.mimeType}` : ""}]`;
      return "";
    }).filter(Boolean).join("\n\n");
  }
  return [...item.chunks.entries()].sort((a, b) => a[0] - b[0]).map((entry) => entry[1]).join("");
}

function safeJson(value) {
  try {
    const text = JSON.stringify(value, null, 2);
    return text.length > 8000 ? `${text.slice(0, 8000)}\n…` : text;
  } catch { return String(value); }
}

function previewText(value, limit = 140) {
  const compact = String(value || "").replace(/\s+/g, " ").trim();
  return compact.length > limit ? `${compact.slice(0, limit)}…` : compact || "No output";
}

function compactDetails(id, label, preview, fullText) {
  const details = node("details", "compact-details");
  details.open = state.expanded.has(id);
  details.addEventListener("toggle", () => {
    if (details.open) state.expanded.add(id);
    else state.expanded.delete(id);
  });
  const summary = node("summary");
  summary.append(node("strong", "", label), node("span", "compact-preview", preview));
  details.append(summary, node("pre", "compact-full", fullText));
  return details;
}

function renderTranscript() {
  const nearBottom = transcript.scrollHeight - transcript.scrollTop - transcript.clientHeight < 120;
  const shouldFollow = state.followLatest || nearBottom;
  const fragment = document.createDocumentFragment();
  let visible = 0;
  for (const id of state.timeline) {
    const item = state.items.get(id);
    if (!item) continue;
    const wrapper = node("article", "timeline-item");
    if (item.kind === "message") {
      const text = messageText(item);
      if (!text && item.done) continue;
      const bubble = node("div", `message ${item.role || "assistant"}`);
      if (item.role === "toolResult") {
        bubble.append(compactDetails(
          item.id,
          `${item.toolName || "tool"} response`,
          previewText(text),
          text || "No output",
        ));
      } else {
        bubble.append(node("div", "message-role", item.role || "assistant"));
        bubble.append(node("p", "message-text", text || "…"));
      }
      const metaBits = [item.model, item.done ? item.stopReason : "streaming"].filter(Boolean);
      if (item.errorMessage) metaBits.push(item.errorMessage);
      if (metaBits.length) bubble.append(node("div", "message-meta", metaBits.join(" · ")));
      wrapper.append(bubble);
    } else if (item.kind === "tool") {
      const card = node("div", `tool-card ${item.error ? "error" : ""}`);
      card.append(node("span", "", item.done ? (item.error ? "×" : "✓") : "◌"));
      const args = item.args === undefined ? "No arguments" : safeJson(item.args);
      card.append(compactDetails(
        item.id,
        item.name,
        item.args === undefined ? (item.done ? "completed" : "running") : previewText(args),
        args,
      ));
      wrapper.append(card);
    } else {
      const label = item.detail ? `${item.type} · ${item.detail}` : item.type;
      wrapper.append(node("div", "lifecycle-card", label.replaceAll("-", " ")));
    }
    fragment.append(wrapper);
    visible += 1;
  }
  transcript.replaceChildren();
  if (visible === 0) transcript.append(node("div", "empty-transcript", "No transcript events yet. Send a prompt or continue in Pi."));
  else transcript.append(fragment);
  if (shouldFollow) requestAnimationFrame(() => { transcript.scrollTop = transcript.scrollHeight; });
  jumpLatest.classList.toggle("hidden", shouldFollow);
}

function closeDetail() {
  closeStream();
  state.selected = null;
  detailView.classList.add("hidden");
  listView.classList.remove("hidden");
  backButton.classList.add("hidden");
  refreshButton.classList.remove("hidden");
  title.textContent = "Pi sessions";
  subtitle.textContent = "Worker Harness";
  setConnection("online");
  renderSessionList();
}

function route() {
  const match = location.hash.match(/^#session\/(.+)$/);
  if (match) openSession(decodeURIComponent(match[1]));
  else closeDetail();
}

async function queueConfiguration(payload) {
  if (!state.selected) return;
  modelControl.disabled = true;
  thinkingControl.disabled = true;
  controlStatus.textContent = "Applying…";
  try {
    await api(`/api/v1/pi/sessions/${encodeURIComponent(state.selected.id)}:configure`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    controlStatus.textContent = "Queued";
  } catch (error) {
    controlStatus.textContent = `Failed: ${error.message}`;
    modelControl.disabled = false;
    thinkingControl.disabled = false;
  }
}

modelControl.addEventListener("change", () => {
  const separator = modelControl.value.indexOf("::");
  if (separator < 1) return;
  void queueConfiguration({
    provider: modelControl.value.slice(0, separator),
    model: modelControl.value.slice(separator + 2),
  });
});
thinkingControl.addEventListener("change", () => {
  void queueConfiguration({ thinking_level: thinkingControl.value });
});

composer.addEventListener("submit", async (event) => {
  event.preventDefault();
  const message = messageInput.value.trim();
  if (!message || !state.selected) return;
  sendButton.disabled = true;
  composerStatus.textContent = "Queueing message…";
  try {
    await api(`/api/v1/pi/sessions/${encodeURIComponent(state.selected.id)}:prompt`, {
      method: "POST",
      body: JSON.stringify({ message, deliver_as: delivery.value }),
    });
    messageInput.value = "";
    resizeComposer();
    composerStatus.textContent = delivery.value === "steer" ? "Steer queued" : "Follow-up queued";
    setTimeout(() => { composerStatus.textContent = ""; }, 2500);
  } catch (error) {
    composerStatus.textContent = `Send failed: ${error.message}`;
  } finally {
    sendButton.disabled = false;
    messageInput.focus();
  }
});

function resizeComposer() {
  messageInput.style.height = "auto";
  messageInput.style.height = `${Math.min(messageInput.scrollHeight, 150)}px`;
}

messageInput.addEventListener("input", resizeComposer);
messageInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    composer.requestSubmit();
  }
});
transcript.addEventListener("scroll", () => {
  state.followLatest = transcript.scrollHeight - transcript.scrollTop - transcript.clientHeight < 80;
  jumpLatest.classList.toggle("hidden", state.followLatest);
}, { passive: true });
jumpLatest.addEventListener("click", () => {
  state.followLatest = true;
  transcript.scrollTo({ top: transcript.scrollHeight, behavior: "smooth" });
  jumpLatest.classList.add("hidden");
});
backButton.addEventListener("click", () => { location.hash = ""; });
refreshButton.addEventListener("click", () => loadSessions());
window.addEventListener("hashchange", route);
window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  state.installPrompt = event;
  installButton.classList.remove("hidden");
});
installButton.addEventListener("click", async () => {
  if (!state.installPrompt) return;
  await state.installPrompt.prompt();
  state.installPrompt = null;
  installButton.classList.add("hidden");
});

if ("serviceWorker" in navigator) navigator.serviceWorker.register("/sw.js").catch(console.warn);
await loadSessions();
route();
setInterval(() => loadSessions({ quiet: true }), 15_000);
setInterval(() => { if (!state.selected) renderSessionList(); }, 30_000);
