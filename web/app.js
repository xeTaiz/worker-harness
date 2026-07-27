const $ = (selector) => document.querySelector(selector);
const listView = $("#session-list-view");
const detailView = $("#session-detail-view");
const sessionList = $("#session-list");
const emptyState = $("#empty-state");
const transcript = $("#transcript");
const meta = $("#session-meta");
const sessionControls = $("#session-controls");
const sessionModes = $("#session-modes");
const transcriptMode = $("#transcript-mode");
const terminalMode = $("#terminal-mode");
const terminalPanel = $("#terminal-panel");
const terminalOutput = $("#terminal-output");
const terminalStatus = $("#terminal-status");
const terminalReconnect = $("#terminal-reconnect");
const terminalDisconnect = $("#terminal-disconnect");
const terminalComposer = $("#terminal-composer");
const terminalInput = $("#terminal-input");
const modelControl = $("#model-control");
const thinkingControl = $("#thinking-control");
const controlStatus = $("#control-status");
const backButton = $("#back");
const refreshButton = $("#refresh");
const title = $("#page-title");
const subtitle = $("#page-subtitle");
const sessionSwitcher = $("#session-switcher");
const sessionPicker = $("#session-picker");
const previousSession = $("#previous-session");
const nextSession = $("#next-session");
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
  attachInfo: null,
  terminalSocket: null,
  terminalCells: [[]],
  terminalRow: 0,
  terminalCol: 0,
  terminalRows: 24,
  terminalCols: 80,
  terminalPending: "",
  terminalSavedCursor: [0, 0],
  terminalDecoder: new TextDecoder(),
  viewMode: "transcript",
  navigation: 0,
  showHistory: false,
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

function isInternalSession(session) {
  return session.name?.startsWith("subagent-");
}

function isHistoricalSession(session) {
  return ["stopped", "failed", "termination_unknown"].includes(session.state);
}

function switchableSessions() {
  const sessions = state.sessions.filter((session) => !isInternalSession(session) && !isHistoricalSession(session));
  if (state.selected && !isInternalSession(state.selected) && !sessions.some((session) => session.id === state.selected.id)) {
    sessions.push(state.selected);
  }
  return sessions.sort((left, right) => {
    const byLabel = sessionLabel(left).localeCompare(sessionLabel(right));
    return byLabel || left.id.localeCompare(right.id);
  });
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
  for (const session of visible) {
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
    foot.append(
      node("span", "", `${session.session_type} · ${session.id.slice(0, 6)}`),
      node("span", "", relativeTime(session.updated_at || session.created_at)),
    );
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
  renderSessionSwitcher();
  renderSessionControls();
}

function renderSessionSwitcher() {
  if (!state.selected) return;
  const sessions = switchableSessions();
  sessionPicker.replaceChildren();
  for (const session of sessions) {
    const option = node(
      "option",
      "",
      `${sessionLabel(session)} · ${session.state.replaceAll("_", " ")} · ${session.id.slice(0, 6)}`,
    );
    option.value = session.id;
    option.selected = session.id === state.selected.id;
    sessionPicker.append(option);
  }
  previousSession.disabled = sessions.length < 2;
  nextSession.disabled = sessions.length < 2;
}

function cycleSession(direction) {
  if (!state.selected) return;
  const sessions = switchableSessions();
  if (sessions.length < 2) return;
  const current = Math.max(0, sessions.findIndex((session) => session.id === state.selected.id));
  const target = sessions[(current + direction + sessions.length) % sessions.length];
  if (target) location.hash = `session/${encodeURIComponent(target.id)}`;
}

async function loadAttachInfo(sessionId) {
  let info;
  try {
    info = await api(`/api/v1/pi/sessions/${encodeURIComponent(sessionId)}/attach-info`);
  } catch (error) {
    info = { attachable: false, reason: error.message };
  }
  if (state.selected?.id !== sessionId) return;
  state.attachInfo = info;
  const attachable = Boolean(state.attachInfo?.attachable);
  sessionModes.classList.toggle("hidden", !attachable);
  terminalMode.title = attachable ? "Attach directly to the worker terminal" : String(state.attachInfo?.reason || "Unavailable");
}

function setSessionMode(mode) {
  const terminal = mode === "terminal" && state.attachInfo?.attachable;
  state.viewMode = terminal ? "terminal" : "transcript";
  transcript.classList.toggle("hidden", terminal);
  terminalPanel.classList.toggle("hidden", !terminal);
  composer.classList.toggle("hidden", terminal);
  jumpLatest.classList.toggle("hidden", terminal || state.followLatest);
  transcriptMode.classList.toggle("active", !terminal);
  transcriptMode.setAttribute("aria-pressed", String(!terminal));
  terminalMode.classList.toggle("active", terminal);
  terminalMode.setAttribute("aria-pressed", String(terminal));
  if (terminal) {
    connectTerminal();
    requestAnimationFrame(() => terminalOutput.focus());
  }
}

function resetTerminalScreen(rows = state.terminalRows, cols = state.terminalCols) {
  state.terminalRows = rows;
  state.terminalCols = cols;
  state.terminalCells = Array.from({ length: rows }, () => []);
  state.terminalRow = 0;
  state.terminalCol = 0;
  state.terminalPending = "";
  state.terminalSavedCursor = [0, 0];
  terminalOutput.textContent = "";
}

function renderTerminalScreen() {
  terminalOutput.textContent = state.terminalCells
    .map((line) => line.join("").replace(/\s+$/, ""))
    .join("\n")
    .replace(/\n+$/, "");
}

function ensureTerminalLine(row = state.terminalRow) {
  while (state.terminalCells.length <= row) state.terminalCells.push([]);
  return state.terminalCells[row];
}

function terminalLineFeed() {
  if (state.terminalRow >= state.terminalRows - 1) {
    state.terminalCells.shift();
    state.terminalCells.push([]);
  } else {
    state.terminalRow += 1;
  }
}

function putTerminalCharacter(character) {
  if (state.terminalCol >= state.terminalCols) {
    state.terminalCol = 0;
    terminalLineFeed();
  }
  const line = ensureTerminalLine();
  while (line.length < state.terminalCol) line.push(" ");
  line[state.terminalCol] = character;
  state.terminalCol += 1;
}

function handleTerminalCsi(body, final) {
  const privateMode = body.startsWith("?");
  const clean = privateMode ? body.slice(1) : body;
  const values = clean.split(";").map((value) => Number.parseInt(value, 10));
  const value = (index = 0, fallback = 1) => Number.isFinite(values[index]) && values[index] !== 0 ? values[index] : fallback;
  const clampCursor = () => {
    state.terminalRow = Math.max(0, Math.min(state.terminalRows - 1, state.terminalRow));
    state.terminalCol = Math.max(0, Math.min(state.terminalCols - 1, state.terminalCol));
  };

  if (final === "A") state.terminalRow -= value();
  else if (final === "B") state.terminalRow += value();
  else if (final === "C") state.terminalCol += value();
  else if (final === "D") state.terminalCol -= value();
  else if (final === "E") { state.terminalRow += value(); state.terminalCol = 0; }
  else if (final === "F") { state.terminalRow -= value(); state.terminalCol = 0; }
  else if (final === "G") state.terminalCol = value(0, 1) - 1;
  else if (final === "d") state.terminalRow = value(0, 1) - 1;
  else if (final === "H" || final === "f") {
    state.terminalRow = value(0, 1) - 1;
    state.terminalCol = value(1, 1) - 1;
  } else if (final === "J") {
    const mode = Number.isFinite(values[0]) ? values[0] : 0;
    if (mode === 2 || mode === 3) {
      state.terminalCells = Array.from({ length: state.terminalRows }, () => []);
    } else if (mode === 0) {
      ensureTerminalLine().splice(state.terminalCol);
      for (let row = state.terminalRow + 1; row < state.terminalRows; row += 1) state.terminalCells[row] = [];
    } else if (mode === 1) {
      for (let row = 0; row < state.terminalRow; row += 1) state.terminalCells[row] = [];
      ensureTerminalLine().fill(" ", 0, state.terminalCol + 1);
    }
  } else if (final === "K") {
    const mode = Number.isFinite(values[0]) ? values[0] : 0;
    const line = ensureTerminalLine();
    if (mode === 0) line.splice(state.terminalCol);
    else if (mode === 1) line.fill(" ", 0, state.terminalCol + 1);
    else if (mode === 2) state.terminalCells[state.terminalRow] = [];
  } else if (final === "P") {
    ensureTerminalLine().splice(state.terminalCol, value());
  } else if (final === "@") {
    ensureTerminalLine().splice(state.terminalCol, 0, ...Array(value()).fill(" "));
  } else if (final === "X") {
    const line = ensureTerminalLine();
    while (line.length < state.terminalCol + value()) line.push(" ");
    line.fill(" ", state.terminalCol, state.terminalCol + value());
  } else if (final === "s") {
    state.terminalSavedCursor = [state.terminalRow, state.terminalCol];
  } else if (final === "u") {
    [state.terminalRow, state.terminalCol] = state.terminalSavedCursor;
  }
  // SGR colors and private mode changes are intentionally ignored; cursor and
  // erase operations are retained so spinners and full-screen redraws update.
  clampCursor();
}

function appendTerminalOutput(text) {
  const input = state.terminalPending + text;
  let index = 0;
  while (index < input.length) {
    const character = input[index];
    if (character === "\x1b") {
      if (index + 1 >= input.length) break;
      const next = input[index + 1];
      if (next === "[") {
        let end = index + 2;
        while (end < input.length && !(input.charCodeAt(end) >= 0x40 && input.charCodeAt(end) <= 0x7e)) end += 1;
        if (end >= input.length) break;
        handleTerminalCsi(input.slice(index + 2, end), input[end]);
        index = end + 1;
        continue;
      }
      if (next === "]") {
        let end = index + 2;
        while (end < input.length && input[end] !== "\x07" && !(input[end] === "\x1b" && input[end + 1] === "\\")) end += 1;
        if (end >= input.length) break;
        index = input[end] === "\x07" ? end + 1 : end + 2;
        continue;
      }
      if (next === "(" || next === ")") {
        if (index + 2 >= input.length) break;
        index += 3;
        continue;
      }
      if (next === "7") state.terminalSavedCursor = [state.terminalRow, state.terminalCol];
      else if (next === "8") [state.terminalRow, state.terminalCol] = state.terminalSavedCursor;
      else if (next === "c") resetTerminalScreen();
      index += 2;
      continue;
    }
    if (character === "\r") state.terminalCol = 0;
    else if (character === "\n") terminalLineFeed();
    else if (character === "\b") state.terminalCol = Math.max(0, state.terminalCol - 1);
    else if (character === "\t") state.terminalCol = Math.min(state.terminalCols - 1, (Math.floor(state.terminalCol / 8) + 1) * 8);
    else if (character >= " " && character !== "\x7f") putTerminalCharacter(character);
    index += 1;
  }
  state.terminalPending = input.slice(index);
  renderTerminalScreen();
}

function disconnectTerminal(reason = "Disconnected") {
  const socket = state.terminalSocket;
  state.terminalSocket = null;
  if (socket && socket.readyState < WebSocket.CLOSING) socket.close(1000, "client disconnect");
  terminalStatus.textContent = reason;
  terminalStatus.className = "";
}

function resizeTerminal() {
  const rows = Math.min(1000, Math.max(8, Math.floor(terminalOutput.clientHeight / 18)));
  const cols = Math.min(1000, Math.max(20, Math.floor(terminalOutput.clientWidth / 8)));
  if (rows !== state.terminalRows || cols !== state.terminalCols) resetTerminalScreen(rows, cols);
  const socket = state.terminalSocket;
  if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ type: "resize", rows, cols }));
}

function connectTerminal() {
  if (!state.attachInfo?.attachable || !state.attachInfo.websocket_url) return;
  if (state.terminalSocket && state.terminalSocket.readyState <= WebSocket.OPEN) return;
  disconnectTerminal("Connecting…");
  resetTerminalScreen();
  state.terminalDecoder = new TextDecoder();
  terminalStatus.className = "connecting";
  const socket = new WebSocket(state.attachInfo.websocket_url);
  socket.binaryType = "arraybuffer";
  state.terminalSocket = socket;
  socket.addEventListener("open", () => {
    if (state.terminalSocket !== socket) return;
    terminalStatus.textContent = "Connected · direct worker relay";
    terminalStatus.className = "online";
    resizeTerminal();
  });
  socket.addEventListener("message", async (event) => {
    if (state.terminalSocket !== socket) return;
    if (typeof event.data === "string") {
      try {
        const payload = JSON.parse(event.data);
        if (payload.type === "status") {
          terminalStatus.textContent = `${payload.state || "connected"} · terminal ${payload.terminal || "ready"}`;
          terminalStatus.className = "online";
        } else if (payload.type === "error") {
          appendTerminalOutput(`\n[relay error: ${payload.detail || payload.message || "unknown error"}]\n`);
        }
      } catch {
        appendTerminalOutput(event.data);
      }
      return;
    }
    const bytes = event.data instanceof Blob ? await event.data.arrayBuffer() : event.data;
    appendTerminalOutput(state.terminalDecoder.decode(bytes, { stream: true }));
  });
  socket.addEventListener("close", (event) => {
    if (state.terminalSocket !== socket) return;
    state.terminalSocket = null;
    terminalStatus.textContent = event.code === 1000 ? "Disconnected" : `Disconnected · code ${event.code}`;
    terminalStatus.className = "";
  });
  socket.addEventListener("error", () => {
    if (state.terminalSocket !== socket) return;
    terminalStatus.textContent = "Connection failed";
    terminalStatus.className = "error";
  });
}

function sendTerminalInput(data) {
  const socket = state.terminalSocket;
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    terminalStatus.textContent = "Not connected";
    terminalStatus.className = "error";
    return false;
  }
  socket.send(JSON.stringify({ type: "input", data }));
  return true;
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
  closeStream();
  listView.classList.add("hidden");
  detailView.classList.remove("hidden");
  backButton.classList.remove("hidden");
  sessionSwitcher.classList.remove("hidden");
  document.body.classList.add("session-open");
  refreshButton.classList.add("hidden");
  transcript.replaceChildren(node("div", "empty-transcript", "Loading durable session history…"));
  state.cursor = 0;
  state.timeline = [];
  state.items.clear();
  state.expanded.clear();
  state.settings = null;
  state.attachInfo = null;
  resetTerminalScreen();
  disconnectTerminal();
  sessionModes.classList.add("hidden");
  setSessionMode("transcript");
  sessionControls.classList.add("hidden");
  controlStatus.textContent = "";
  state.followLatest = true;
  setConnection("connecting");
  try {
    state.selected = state.sessions.find((session) => session.id === id) || await api(`/api/v1/pi/sessions/${encodeURIComponent(id)}`);
    if (state.navigation !== navigation) return;
    renderSessionHeader();
    const attachInfo = loadAttachInfo(id);
    if (!await replayEvents(id, navigation)) return;
    await attachInfo;
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
  state.navigation += 1;
  closeStream();
  disconnectTerminal();
  state.selected = null;
  state.attachInfo = null;
  sessionModes.classList.add("hidden");
  setSessionMode("transcript");
  detailView.classList.add("hidden");
  listView.classList.remove("hidden");
  backButton.classList.add("hidden");
  sessionSwitcher.classList.add("hidden");
  document.body.classList.remove("session-open");
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
sessionPicker.addEventListener("change", () => {
  if (sessionPicker.value && sessionPicker.value !== state.selected?.id) {
    location.hash = `session/${encodeURIComponent(sessionPicker.value)}`;
  }
});
previousSession.addEventListener("click", () => cycleSession(-1));
nextSession.addEventListener("click", () => cycleSession(1));
transcriptMode.addEventListener("click", () => setSessionMode("transcript"));
terminalMode.addEventListener("click", () => setSessionMode("terminal"));
terminalReconnect.addEventListener("click", connectTerminal);
terminalDisconnect.addEventListener("click", () => disconnectTerminal());
terminalComposer.addEventListener("submit", (event) => {
  event.preventDefault();
  const value = terminalInput.value;
  if (!value || !sendTerminalInput(`${value}\r`)) return;
  terminalInput.value = "";
});
for (const button of document.querySelectorAll("[data-terminal-key]")) {
  button.addEventListener("click", () => {
    try { sendTerminalInput(JSON.parse(`"${button.dataset.terminalKey}"`)); } catch { /* invalid key mapping */ }
    terminalOutput.focus();
  });
}
if ("ResizeObserver" in window) new ResizeObserver(resizeTerminal).observe(terminalOutput);
terminalOutput.addEventListener("keydown", (event) => {
  let data = "";
  if (event.ctrlKey && !event.altKey && !event.metaKey && event.key.length === 1) {
    const code = event.key.toUpperCase().charCodeAt(0);
    if (code >= 64 && code <= 95) data = String.fromCharCode(code - 64);
  } else if (!event.ctrlKey && !event.altKey && !event.metaKey) {
    const keys = {
      Enter: "\r", Backspace: "\x7f", Tab: "\t", Escape: "\x1b",
      ArrowUp: "\x1b[A", ArrowDown: "\x1b[B", ArrowRight: "\x1b[C", ArrowLeft: "\x1b[D",
      Home: "\x1b[H", End: "\x1b[F", Delete: "\x1b[3~",
    };
    data = keys[event.key] || (event.key.length === 1 ? event.key : "");
  }
  if (!data) return;
  event.preventDefault();
  sendTerminalInput(data);
});
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
