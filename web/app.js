const $ = (selector) => document.querySelector(selector);
const listView = $("#session-list-view");
const globalView = $("#global-view");
const detailView = $("#session-detail-view");
const sessionList = $("#session-list");
const emptyState = $("#empty-state");
const transcript = $("#transcript");
const sessionMeta = $("#session-meta");
const sessionControls = $("#session-controls");
const modelControl = $("#model-control");
const thinkingControl = $("#thinking-control");
const controlStatus = $("#control-status");
const refreshButton = $("#refresh");
const title = $("#page-title");
const subtitle = $("#page-subtitle");
const agentSidebar = $("#agent-sidebar");
const sidebarToggle = $("#sidebar-toggle");
const sidebarClose = $("#sidebar-close");
const sidebarBackdrop = $("#sidebar-backdrop");
const mobileSidebar = window.matchMedia("(max-width: 759px)");
const connectionDot = $("#connection-dot");
const composer = $("#composer");
const messageInput = $("#message");
const sendButton = $("#send");
const composerStatus = $("#composer-status");
const delivery = $("#delivery");
const jumpLatest = $("#jump-latest");
const installButton = $("#install");
const globalButton = $("#global-button");
const globalRefresh = $("#global-refresh");
const globalRoster = $("#global-roster");
const globalTurns = $("#global-turns");
const globalComposer = $("#global-composer");
const globalMessage = $("#global-message");
const globalSend = $("#global-send");
const globalStatus = $("#global-status");

const state = {
  sessions: [],
  selected: null,
  cursor: 0,
  source: null,
  timeline: [],
  items: new Map(),
  expanded: new Set(),
  settings: null,
  navigation: 0,
  sidebarOpen: false,
  showHistory: false,
  followLatest: true,
  installPrompt: null,
  globalOpen: false,
  globalExpanded: new Set(),
  globalRenderTimer: null,
  globalLoading: false,
  globalGeneration: 0,
  globalSources: new Map(),
  globalSending: false,
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
function roleBadge(session) {
  const role = ["orchestrator", "pm", "task"].includes(session.role) ? session.role : "";
  return role ? node("span", `role-badge role-${role}`, role) : null;
}

function sessionFacts(session, { linkPr = true } = {}) {
  const meta = session.meta && typeof session.meta === "object" ? session.meta : {};
  const facts = node("div", "session-facts");
  if (meta.project) facts.append(node("span", "session-fact", `Project: ${meta.project}`));
  if (meta.branch) facts.append(node("span", "session-fact", `Branch: ${meta.branch}`));
  if (meta.pr_url) {
    if (linkPr) {
      const link = node("a", "session-pr-link", "Open PR");
      try {
        const url = new URL(String(meta.pr_url), location.origin);
        if (!["http:", "https:"].includes(url.protocol)) throw new Error("Unsupported URL");
        link.href = url.href;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        facts.append(link);
      } catch {
        facts.append(node("span", "session-fact", "PR available"));
      }
    } else {
      facts.append(node("span", "session-fact", "PR available"));
    }
  }
  return facts.childElementCount ? facts : null;
}

function renderSessionMeta() {
  if (!state.selected) return;
  sessionMeta.replaceChildren();
  const badges = node("div", "session-meta-badges");
  badges.append(
    node("span", `session-state-badge ${state.selected.state}`, state.selected.state.replaceAll("_", " ")),
  );
  const role = roleBadge(state.selected);
  if (role) badges.prepend(role);
  sessionMeta.append(badges);
  if (state.selected.question) {
    sessionMeta.append(node("p", "session-question", `Waiting on: ${state.selected.question}`));
  }
  const facts = sessionFacts(state.selected);
  if (facts) sessionMeta.append(facts);
  sessionMeta.classList.toggle("hidden", sessionMeta.childElementCount === 0);
  sessionMeta.setAttribute("aria-hidden", String(sessionMeta.childElementCount === 0));
  sessionMeta.classList.toggle("blocked", state.selected.state === "blocked");
}


function isInternalSession(session) {
  return session.name?.startsWith("subagent-");
}

function isHistoricalSession(session) {
  return ["stopped", "failed", "termination_unknown"].includes(session.state);
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
    const previous = new Map(state.sessions.map((session) => [session.id, session]));
    const loaded = await api("/api/v1/pi/sessions");
    state.sessions = loaded.map((session) => ({ ...previous.get(session.id), ...session }));
    state.sessions.sort((a, b) => (b.updated_at || b.created_at) - (a.updated_at || a.created_at));
    renderSessionList();
    if (state.selected) {
      const updated = state.sessions.find((session) => session.id === state.selected.id);
      if (updated) {
        state.selected = updated;
        renderSessionHeader();
      }
    }
    if (state.globalOpen) {
      renderGlobal();
      connectGlobalSources();
    }
    if (!state.selected) setConnection("online");
    return true;
  } catch (error) {
    console.warn("Unable to load sessions", error);
    setConnection("offline");
    if (!quiet) showListError(error.message);
    return false;
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
  const operatorSessions = state.sessions.filter((session) => !isInternalSession(session));
  const historical = operatorSessions.filter(isHistoricalSession);
  const current = operatorSessions.filter((session) => !isHistoricalSession(session));
  const visible = state.showHistory ? operatorSessions : current;
  const working = current.filter((session) => session.state === "working").length;
  const historyToggle = node("button", "summary-pill history-toggle", `${state.showHistory ? "Hide history" : "History"} ${historical.length}`);
  historyToggle.type = "button";
  historyToggle.setAttribute("aria-pressed", String(state.showHistory));
  historyToggle.addEventListener("click", () => {
    state.showHistory = !state.showHistory;
    renderSessionList();
  });
  $("#summary").replaceChildren(
    summaryPill(current.length, "current"),
    summaryPill(working, "working"),
    historyToggle,
  );
  emptyState.classList.toggle("hidden", visible.length !== 0);
  globalButton.classList.toggle("active", state.globalOpen);
  globalButton.setAttribute("aria-current", state.globalOpen ? "page" : "false");
  for (const session of visible) {
    const card = node("button", `session-card${session.id === state.selected?.id ? " active" : ""}${session.state === "blocked" ? " blocked" : ""}`);
    card.type = "button";
    if (session.id === state.selected?.id) card.setAttribute("aria-current", "page");
    card.addEventListener("click", () => {
      setSidebarOpen(false);
      location.hash = `session/${encodeURIComponent(session.id)}`;
    });
    const head = node("div", "session-card-head");
    head.append(
      node("span", `state-dot ${session.state}`),
      node("span", "session-name", sessionLabel(session)),
      node("span", "session-state", session.state.replaceAll("_", " ")),
    );
    const role = roleBadge(session);
    if (role) head.insertBefore(role, head.lastElementChild);
    const context = node("p", "session-context", sessionContext(session));
    const foot = node("div", "session-foot");
    foot.append(
      node("span", "", `${session.session_type} · ${session.id.slice(0, 6)}`),
      node("span", "", relativeTime(session.updated_at || session.created_at)),
    );
    card.append(head, context);
    if (session.question) card.append(node("p", "session-question", `Waiting on: ${session.question}`));
    const facts = sessionFacts(session, { linkPr: false });
    if (facts) card.append(facts);
    card.append(foot);
    sessionList.append(card);
  }
}

function summaryPill(value, label) {
  const pill = node("span", "summary-pill");
  const strong = node("strong", "", String(value));
  pill.append(strong, document.createTextNode(` ${label}`));
  return pill;
}

function closeGlobalSources() {
  for (const source of state.globalSources.values()) source.close();
  state.globalSources.clear();
}

function globalInteractiveSessions() {
  return state.sessions.filter((session) => !isInternalSession(session) && !isHistoricalSession(session));
}

function orchestratorSession() {
  return globalInteractiveSessions().find((session) => session.role === "orchestrator") || null;
}

function renderGlobalComposer() {
  globalMessage.disabled = state.globalSending;
  globalSend.disabled = state.globalSending;
  globalMessage.placeholder = "Message the orchestrator…";
}

function renderGlobal() {
  const sessions = globalInteractiveSessions();
  const activeIds = new Set(sessions.map((session) => session.id));
  for (const id of state.globalExpanded) {
    if (!activeIds.has(id)) state.globalExpanded.delete(id);
  }
  renderGlobalComposer();
  globalRoster.replaceChildren();
  globalTurns.replaceChildren();
  if (!sessions.length) {
    globalRoster.append(node("p", "global-empty", "No active sessions."));
    globalTurns.append(node("p", "global-empty", "Start an agent to populate the fleet view."));
    return;
  }
  for (const session of sessions) {
    const row = node("article", `global-agent-row${session.state === "blocked" ? " blocked" : ""}`);
    const identity = node("button", "global-agent-identity");
    identity.type = "button";
    identity.addEventListener("click", () => { location.hash = `session/${encodeURIComponent(session.id)}`; });
    const heading = node("div", "global-agent-title");
    heading.append(
      node("span", `state-dot ${session.state}`),
      node("strong", "", sessionLabel(session)),
    );
    const role = roleBadge(session);
    if (role) heading.append(role);
    heading.append(node("span", "global-agent-host", session.host || "local"));
    identity.append(heading);
    if (session.question) {
      identity.append(node("p", "session-question", `Waiting on: ${session.question}`));
    } else {
      identity.append(node("p", "global-agent-prompt", String(session.latest_user_prompt || "No prompt captured yet").slice(-240)));
    }
    const facts = sessionFacts(session, { linkPr: false });
    if (facts) identity.append(facts);
    const actions = node("div", "global-agent-actions");
    if (session.current_tool) actions.append(node("span", "tool-badge active", session.current_tool));
    for (const tool of (session.recent_tools || []).filter((tool) => tool !== session.current_tool).slice(0, 2)) {
      actions.append(node("span", "tool-badge", tool));
    }
    if (session.has_pending_messages) actions.append(node("span", "queued-badge", "queued"));
    const linkedFacts = sessionFacts(session);
    const prLink = linkedFacts?.querySelector(".session-pr-link");
    if (prLink) actions.append(prLink);
    if (session.state === "working" || session.has_pending_messages) {
      const interrupt = node("button", "interrupt-button", "Interrupt");
      interrupt.type = "button";
      interrupt.addEventListener("click", () => void interruptSession(session.id));
      actions.append(interrupt);
    }
    row.append(identity, actions);
    globalRoster.append(row);
  }

  const ordered = [...sessions]
    .sort((a, b) => (b.last_user_at || b.updated_at || 0) - (a.last_user_at || a.updated_at || 0))
    .slice(0, 24);
  for (const session of ordered) {
    const card = node("details", `global-turn-card${session.state === "blocked" ? " blocked" : ""}`);
    card.open = state.globalExpanded.has(session.id);
    card.addEventListener("toggle", () => {
      if (card.open) state.globalExpanded.add(session.id);
      else state.globalExpanded.delete(session.id);
    });
    const summary = node("summary");
    const heading = node("div", "global-turn-heading");
    heading.append(node("span", `state-dot ${session.state}`), node("strong", "", sessionLabel(session)));
    const role = roleBadge(session);
    if (role) heading.append(role);
    const promptText = session.question
      ? `Waiting on: ${session.question}`
      : String(session.latest_user_prompt || "No prompt captured yet").slice(-240);
    const prompt = node("p", session.question ? "global-turn-prompt session-question" : "global-turn-prompt", promptText);
    const output = node("p", "global-turn-output", String(session.assistant_tail || "Waiting for output…").slice(-1200));
    const tools = node("div", "global-turn-tools");
    for (const tool of (session.recent_tools || []).slice(0, 3)) tools.append(node("span", "tool-badge", tool));
    if (session.current_tool) tools.append(node("span", "tool-badge active", `${session.current_tool} running`));
    const time = session.last_user_at || session.updated_at;
    summary.append(heading, prompt, output, tools, node("span", "global-turn-meta", `${relativeTime(time)} · Expand`));
    const expanded = node("div", "global-turn-expanded");
    const open = node("button", "open-session-button", "Open transcript");
    open.type = "button";
    open.addEventListener("click", () => { location.hash = `session/${encodeURIComponent(session.id)}`; });
    expanded.append(open);
    card.append(summary, expanded);
    globalTurns.append(card);
  }
}

function scheduleGlobalRender() {
  if (state.globalRenderTimer) return;
  state.globalRenderTimer = setTimeout(() => {
    state.globalRenderTimer = null;
    if (state.globalOpen) renderGlobal();
  }, 200);
}

function eventMessageText(payload) {
  const message = payload.message || {};
  return Array.isArray(message.content)
    ? message.content.filter((block) => block?.type === "text").map((block) => String(block.text || "")).join("\n")
    : "";
}

function applyGlobalEvent(sessionId, event) {
  const session = globalInteractiveSessions().find((item) => item.id === sessionId);
  if (!session) return;
  session.cursor = Math.max(Number(session.cursor) || 0, Number(event.sequence) || 0);
  const payload = event.payload || {};
  if (event.event_type === "message-end") {
    const role = payload.message?.role;
    const text = eventMessageText(payload);
    if (role === "user" && text) {
      session.latest_user_prompt = text.slice(-500);
      session.assistant_tail = "";
      session.last_user_at = event.created_at;
    } else if (role === "assistant" && text) {
      session.assistant_tail = text.slice(-1200);
    }
  } else if (event.event_type === "message-delta") {
    session.assistant_tail = `${session.assistant_tail || ""}${payload.delta || ""}`.slice(-1200);
  } else if (event.event_type === "tool-start") {
    session.current_tool = String(payload.tool_name || "tool");
    session.recent_tools = [session.current_tool, ...(session.recent_tools || []).filter((item) => item !== session.current_tool)].slice(0, 3);
  } else if (event.event_type === "tool-end") {
    if (session.current_tool === payload.tool_name) session.current_tool = "";
  } else if (event.event_type === "blocked") {
    session.question = String(payload.question || "");
    session.state = "blocked";
  } else if (event.event_type === "agent-start") {
    session.state = session.question ? "blocked" : "working";
  } else if (event.event_type === "agent-settled") {
    session.state = session.question ? "blocked" : "idle";
    session.has_pending_messages = false;
  } else if (event.event_type === "pending-state") {
    session.has_pending_messages = Boolean(payload.has_pending_messages);
  } else if (event.event_type === "interrupt-applied") {
    session.has_pending_messages = false;
  }
  scheduleGlobalRender();
}

function connectGlobalSources() {
  const activeIds = new Set(globalInteractiveSessions().map((session) => session.id));
  for (const [id, source] of state.globalSources) {
    if (!activeIds.has(id)) {
      source.close();
      state.globalSources.delete(id);
    }
  }
  for (const session of globalInteractiveSessions()) {
    if (state.globalSources.has(session.id)) continue;
    const source = new EventSource(`/api/v1/pi/sessions/${encodeURIComponent(session.id)}/stream?after=${session.cursor || 0}`);
    source.addEventListener("pi-event", (incoming) => {
      try { applyGlobalEvent(session.id, JSON.parse(incoming.data)); } catch { /* malformed event */ }
    });
    state.globalSources.set(session.id, source);
  }
}

async function refreshGlobal() {
  if (state.globalLoading || !state.globalOpen) return;
  state.globalLoading = true;
  try {
    await loadSessions({ quiet: true });
  } finally {
    state.globalLoading = false;
  }
}

async function interruptSession(sessionId) {
  try {
    await api(`/api/v1/pi/sessions/${encodeURIComponent(sessionId)}:interrupt`, { method: "POST", body: "{}" });
    globalStatus.textContent = "Interrupt queued";
  } catch (error) {
    globalStatus.textContent = `Interrupt failed: ${error.message}`;
  }
}

async function openGlobal() {
  state.navigation += 1;
  closeStream();
  state.selected = null;
  if (!state.globalOpen) {
    state.globalOpen = true;
    state.globalGeneration += 1;
  }
  listView.classList.add("hidden");
  detailView.classList.add("hidden");
  globalView.classList.remove("hidden");
  document.body.classList.add("global-open");
  document.body.classList.remove("session-open");
  title.textContent = "Global";
  subtitle.textContent = "Orchestrator · fleet sessions";
  globalButton.classList.add("active");
  globalButton.setAttribute("aria-current", "page");
  if (mobileSidebar.matches) setSidebarOpen(false);
  renderGlobal();
  await refreshGlobal();
}

function closeGlobal() {
  state.globalOpen = false;
  state.globalLoading = false;
  state.globalGeneration += 1;
  globalView.classList.add("hidden");
  document.body.classList.remove("global-open");
  globalButton.classList.remove("active");
  globalButton.setAttribute("aria-current", "false");
  closeGlobalSources();
  if (state.globalRenderTimer) clearTimeout(state.globalRenderTimer);
  state.globalRenderTimer = null;
}

function renderSessionHeader() {
  if (!state.selected) return;
  title.textContent = sessionLabel(state.selected);
  subtitle.replaceChildren();
  const context = [state.selected.host, state.selected.cwd].filter(Boolean);
  if (context.length === 0) context.push(`${state.selected.session_type} · ${state.selected.state.replaceAll("_", " ")}`);
  for (const [index, value] of context.entries()) {
    if (index) subtitle.append(document.createTextNode(" · "));
    subtitle.append(node("span", index === 0 ? "header-host" : "header-cwd", value));
  }
  renderSessionMeta();
  renderSessionControls();
  renderSessionList();
}

function setSidebarOpen(open) {
  state.sidebarOpen = Boolean(open);
  agentSidebar.classList.toggle("open", state.sidebarOpen);
  sidebarBackdrop.classList.toggle("hidden", !state.sidebarOpen);
  sidebarToggle.setAttribute("aria-expanded", String(state.sidebarOpen));
  sidebarToggle.setAttribute("aria-label", state.sidebarOpen ? "Hide agents" : "Show agents");
  agentSidebar.inert = mobileSidebar.matches && !state.sidebarOpen;
  document.body.classList.toggle("sidebar-open", state.sidebarOpen);
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
  const navigation = ++state.navigation;
  closeGlobal();
  closeStream();
  listView.classList.add("hidden");
  detailView.classList.remove("hidden");
  document.body.classList.add("session-open");
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
    if (state.navigation !== navigation) return;
    renderSessionHeader();
    if (!await replayEvents(id, navigation)) return;
    if (state.navigation !== navigation) return;
    renderTranscript();
    connectStream(id);
  } catch (error) {
    if (state.navigation !== navigation) return;
    transcript.replaceChildren(node("div", "empty-transcript", `Unable to open session: ${error.message}`));
    setConnection("offline");
  }
}

async function replayEvents(id, navigation) {
  while (state.navigation === navigation) {
    const events = await api(`/api/v1/pi/sessions/${encodeURIComponent(id)}/events?after=${state.cursor}&limit=500`);
    if (state.navigation !== navigation) return false;
    for (const event of events) applyEvent(event);
    if (events.length < 500) return true;
  }
  return false;
}

function connectStream(id) {
  closeStream();
  const source = new EventSource(`/api/v1/pi/sessions/${encodeURIComponent(id)}/stream?after=${state.cursor}`);
  state.source = source;
  source.addEventListener("open", () => setConnection("online"));
  source.addEventListener("error", () => setConnection("connecting"));
  source.addEventListener("pi-event", (incoming) => {
    if (state.selected?.id !== id) return;
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

async function copyText(text) {
  if (navigator.clipboard && window.isSecureContext) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const input = document.createElement("textarea");
  input.value = text;
  input.setAttribute("readonly", "");
  input.style.position = "fixed";
  input.style.opacity = "0";
  document.body.append(input);
  input.select();
  const copied = document.execCommand("copy");
  input.remove();
  if (!copied) throw new Error("Copy is unavailable in this browser");
}

function renderMarkdown(target, source) {
  const markedApi = globalThis.marked;
  const purifier = globalThis.DOMPurify;
  if (!markedApi?.parse || !purifier?.sanitize) {
    target.textContent = source;
    return;
  }
  try {
    const parsed = markedApi.parse(source, { gfm: true, breaks: true });
    target.innerHTML = purifier.sanitize(parsed, {
      USE_PROFILES: { html: true },
      FORBID_TAGS: ["style", "img", "form", "input", "textarea", "select", "button", "iframe", "object", "embed"],
    });
    for (const link of target.querySelectorAll("a[href]")) {
      link.target = "_blank";
      link.rel = "noopener noreferrer";
    }
    if (typeof globalThis.renderMathInElement === "function") {
      globalThis.renderMathInElement(target, {
        delimiters: [
          { left: "$$", right: "$$", display: true },
          { left: "\\[", right: "\\]", display: true },
          { left: "\\(", right: "\\)", display: false },
          { left: "$", right: "$", display: false },
        ],
        ignoredTags: ["script", "noscript", "style", "textarea", "pre", "code", "option"],
        throwOnError: false,
        trust: false,
        strict: "ignore",
        maxExpand: 1000,
      });
    }
    for (const pre of target.querySelectorAll("pre")) {
      const code = pre.querySelector(":scope > code");
      if (!code) continue;
      const language = [...code.classList]
        .find((name) => name.startsWith("language-"))
        ?.slice("language-".length) || "code";
      const wrapper = node("div", "markdown-code-block");
      const toolbar = node("div", "markdown-code-toolbar");
      const copy = node("button", "markdown-copy", "Copy");
      copy.type = "button";
      copy.setAttribute("aria-label", `Copy ${language} code`);
      copy.addEventListener("click", async () => {
        try {
          await copyText(code.textContent || "");
          copy.textContent = "Copied";
        } catch {
          copy.textContent = "Copy failed";
        }
        setTimeout(() => { copy.textContent = "Copy"; }, 1500);
      });
      toolbar.append(node("span", "markdown-code-language", language), copy);
      pre.replaceWith(wrapper);
      wrapper.append(toolbar, pre);
    }
  } catch {
    target.textContent = source;
  }
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

function isAgentWork(item) {
  if (item.kind !== "message") return true;
  if (item.role === "toolResult") return true;
  if (item.role !== "assistant" || !item.done) return false;
  return String(item.stopReason || "").replaceAll("_", "").toLowerCase() === "tooluse";
}

function renderTimelineItem(item) {
  const wrapper = node("article", "timeline-item");
  if (item.kind === "message") {
    const text = messageText(item);
    if (!text && item.done) return null;
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
      const body = node("div", "message-text markdown-body");
      renderMarkdown(body, text || "…");
      bubble.append(body);
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
  return wrapper;
}

function renderWorkGroup(items) {
  const id = `work:${items[0].id}`;
  const details = node("details", "agent-work-group");
  details.open = state.expanded.has(id);
  details.addEventListener("toggle", () => {
    if (details.open) state.expanded.add(id);
    else state.expanded.delete(id);
  });
  const tools = items.filter((item) => item.kind === "tool").map((item) => item.name);
  const errors = items.filter((item) => item.error || item.errorMessage).length;
  const summary = node("summary", "agent-work-summary");
  summary.append(
    node("span", "agent-work-title", `Agent work · ${items.length}`),
    node("span", "agent-work-preview", [tools.slice(0, 3).join(", "), errors ? `${errors} error${errors === 1 ? "" : "s"}` : ""].filter(Boolean).join(" · ") || "Show intermediate activity"),
  );
  const content = node("div", "agent-work-content");
  for (const item of items) {
    const rendered = renderTimelineItem(item);
    if (rendered) content.append(rendered);
  }
  details.append(summary, content);
  const wrapper = node("section", "timeline-item agent-work-item");
  wrapper.append(details);
  return wrapper;
}

function renderTranscript() {
  const nearBottom = transcript.scrollHeight - transcript.scrollTop - transcript.clientHeight < 120;
  const shouldFollow = state.followLatest || nearBottom;
  const fragment = document.createDocumentFragment();
  let visible = 0;
  let work = [];
  const flushWork = () => {
    if (!work.length) return;
    fragment.append(renderWorkGroup(work));
    visible += 1;
    work = [];
  };
  for (const id of state.timeline) {
    const item = state.items.get(id);
    if (!item) continue;
    if (isAgentWork(item)) {
      work.push(item);
      continue;
    }
    flushWork();
    const rendered = renderTimelineItem(item);
    if (!rendered) continue;
    fragment.append(rendered);
    visible += 1;
  }
  flushWork();
  transcript.replaceChildren();
  if (visible === 0) transcript.append(node("div", "empty-transcript", "No transcript events yet. Send a prompt or continue in Pi."));
  else transcript.append(fragment);
  if (shouldFollow) requestAnimationFrame(() => { transcript.scrollTop = transcript.scrollHeight; });
  jumpLatest.classList.toggle("hidden", shouldFollow);
}

function closeDetail() {
  state.navigation += 1;
  closeGlobal();
  closeStream();
  state.selected = null;
  detailView.classList.add("hidden");
  listView.classList.remove("hidden");
  document.body.classList.remove("session-open");
  title.textContent = "Pi sessions";
  subtitle.textContent = "Worker Harness";
  setConnection("online");
  renderSessionList();
}

function route() {
  if (location.hash === "#global") {
    void openGlobal();
    return;
  }
  const match = location.hash.match(/^#session\/(.+)$/);
  if (match) void openSession(decodeURIComponent(match[1]));
  else closeDetail();
}

globalComposer.addEventListener("submit", async (event) => {
  event.preventDefault();
  const message = globalMessage.value.trim();
  if (!message || state.globalSending) return;
  const generation = state.globalGeneration;
  const starting = !orchestratorSession();
  state.globalSending = true;
  renderGlobalComposer();
  globalStatus.textContent = starting ? "Starting orchestrator…" : "Sending…";
  try {
    const result = await api("/api/v1/pi/orchestrator:send", {
      method: "POST",
      body: JSON.stringify({ message }),
    });
    if (!state.globalOpen || generation !== state.globalGeneration) return;
    if (globalMessage.value.trim() === message) {
      globalMessage.value = "";
      resizeGlobalComposer();
    }
    const session = result.session;
    const existing = state.sessions.findIndex((item) => item.id === session.id);
    if (existing === -1) state.sessions.push(session);
    else state.sessions[existing] = { ...state.sessions[existing], ...session };
    location.hash = `session/${encodeURIComponent(session.id)}`;
  } catch (error) {
    if (state.globalOpen && generation === state.globalGeneration) {
      globalStatus.textContent = `Send failed: ${error.message}`;
    }
  } finally {
    state.globalSending = false;
    if (state.globalOpen && generation === state.globalGeneration) {
      renderGlobalComposer();
      globalMessage.focus();
    }
  }
});

function resizeGlobalComposer() {
  globalMessage.style.height = "auto";
  globalMessage.style.height = `${Math.min(globalMessage.scrollHeight, 150)}px`;
}

globalMessage.addEventListener("input", resizeGlobalComposer);
globalMessage.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    globalComposer.requestSubmit();
  }
});
globalButton.addEventListener("click", () => {
  setSidebarOpen(false);
  location.hash = "global";
});
globalRefresh.addEventListener("click", () => void refreshGlobal());

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
sidebarToggle.addEventListener("click", () => setSidebarOpen(!state.sidebarOpen));
sidebarClose.addEventListener("click", () => setSidebarOpen(false));
sidebarBackdrop.addEventListener("click", () => setSidebarOpen(false));
mobileSidebar.addEventListener("change", () => setSidebarOpen(false));
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && state.sidebarOpen) setSidebarOpen(false);
});
refreshButton.addEventListener("click", () => {
  if (state.globalOpen) void refreshGlobal();
  else void loadSessions();
});
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
setSidebarOpen(false);
await loadSessions();
route();
setInterval(() => loadSessions({ quiet: true }), 15_000);
setInterval(() => { if (state.globalOpen) void refreshGlobal(); }, 10_000);
setInterval(() => { if (!state.selected && !state.globalOpen) renderSessionList(); }, 30_000);
