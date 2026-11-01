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

// Account-scoped state lives only in memory and is discarded on authentication loss.
const workspaceView = $("#workspace-view");
const workspaceContent = $("#workspace-content");
const hub = {
  ready: false, user: null, csrf: "", isBootstrap: false, workers: [], board: null,
  route: "", routeGeneration: 0, identityGeneration: 0, requests: new Set(),
  timer: null, refresh: null, refreshing: null, tokens: new Set(), dialogs: new Set(),
};
const enc = encodeURIComponent;
const when = (seconds) => seconds == null ? "unknown" : new Date(seconds * 1000).toLocaleString();
const runtime = (seconds) => seconds > 0 ? `${Math.round(seconds / 60 * 10) / 10} min` : "unknown";

function notice(message, error = false) {
  const target = $("#workspace-notice");
  target.textContent = message;
  target.classList.toggle("hidden", !message);
  target.classList.toggle("error", error);
}

function button(label, action, className = "") {
  const result = node("button", `action-button ${className}`, label);
  result.type = "button";
  result.addEventListener("click", () => void runAction(result, action));
  return result;
}

async function runAction(control, action) {
  control.disabled = true;
  try { await action(); }
  catch (error) { if (hub.user && error.name !== "AbortError") notice(error.message, true); }
  finally { control.disabled = false; }
}

function field(form, label, name, { type = "text", value = "", required = false, min, max, maxLength, choices, autocomplete } = {}) {
  const wrapper = node("label", "form-field");
  wrapper.append(node("span", "", label));
  const input = node(choices ? "select" : type === "textarea" ? "textarea" : "input");
  input.name = name;
  if (choices) {
    for (const [key, text] of choices) {
      const option = node("option", "", text);
      option.value = key;
      input.append(option);
    }
  } else if (type !== "textarea") input.type = type;
  if (type === "checkbox") input.checked = Boolean(value);
  else input.value = value ?? "";
  input.required = required;
  if (min != null) input.min = min;
  if (max != null) input.max = max;
  if (maxLength != null) input.maxLength = maxLength;
  if (autocomplete) input.autocomplete = autocomplete;
  if (type === "password") { input.minLength = 12; input.maxLength = 1024; }
  wrapper.append(input);
  form.insertBefore(wrapper, form.querySelector(':scope > button[type="submit"]'));
  return input;
}

function formBox(heading, onSubmit, submitLabel = "Create") {
  const form = node("form", "form-grid");
  if (heading) form.append(node("h3", "form-heading", heading));
  const submit = node("button", "action-button primary", submitLabel);
  submit.type = "submit";
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    if (form.reportValidity()) void runAction(submit, () => onSubmit(form));
  });
  form.append(submit);
  return form;
}

function values(form) {
  return Object.fromEntries(new FormData(form));
}

function workerField(form, value = "", all = false) {
  return field(form, "Worker", "worker_id", {
    required: !all, value,
    choices: [["", all ? "All workers" : "Choose a worker"], ...hub.workers.map((worker) => [worker.id, `${worker.name} (${worker.status})`])],
  });
}

function section(heading, parent = workspaceContent) {
  const result = node("section", "workspace-section");
  result.append(node("h2", "", heading));
  parent.append(result);
  return result;
}

function table(headings, rows) {
  const wrapper = node("div", "table-scroll");
  const result = node("table", "data-table");
  const head = node("tr");
  for (const text of headings) { const cell = node("th", "", text); cell.scope = "col"; head.append(cell); }
  const thead = node("thead");
  thead.append(head);
  const body = node("tbody");
  for (const row of rows) {
    const tr = node("tr");
    for (const value of row) {
      const cell = node("td");
      cell.append(value instanceof Node ? value : document.createTextNode(String(value ?? "—")));
      tr.append(cell);
    }
    body.append(tr);
  }
  result.append(thead, body);
  wrapper.append(result);
  return wrapper;
}

function empty(target, message) { target.replaceChildren(node("p", "muted", message)); }
function clearTokens() {
  for (const token of hub.tokens) { token.value = ""; token.closest(".issued-key")?.remove(); }
  hub.tokens.clear();
}
function closeDialogs() {
  for (const dialog of hub.dialogs) { dialog.close(); dialog.remove(); }
  hub.dialogs.clear();
}
function stopPolling() { clearInterval(hub.timer); hub.timer = null; }
function startPolling() {
  stopPolling();
  if (!document.hidden && hub.user) hub.timer = setInterval(() => void refreshVisible(), 5000);
}
async function refreshVisible() {
  if (!hub.refresh || document.hidden || !hub.user) return;
  const generation = hub.routeGeneration;
  if (hub.refreshing === generation) return;
  hub.refreshing = generation;
  try {
    await hub.refresh();
    if (generation === hub.routeGeneration) setConnection("online");
  } catch (error) {
    if (generation === hub.routeGeneration && hub.user && error.name !== "AbortError") {
      notice(error.message, true);
      setConnection("offline");
    }
  } finally { if (hub.refreshing === generation) hub.refreshing = null; }
}

function clearIdentity() {
  hub.identityGeneration += 1;
  hub.routeGeneration += 1;
  for (const request of hub.requests) request.abort();
  hub.requests.clear();
  stopPolling();
  closeStream();
  closeGlobal();
  closeDialogs();
  clearTokens();
  hub.user = null;
  hub.csrf = "";
  hub.isBootstrap = false;
  hub.board = null;
  hub.workers = [];
  hub.refresh = null;
  state.sessions = [];
  state.selected = null;
  state.timeline = [];
  state.items.clear();
  state.expanded.clear();
  state.globalExpanded.clear();
  state.settings = null;
  state.cursor = 0;
  state.navigation += 1;
  messageInput.value = "";
  globalMessage.value = "";
  for (const target of [sessionList, transcript, sessionMeta, globalRoster, globalTurns, workspaceContent]) target.replaceChildren();
  renderNavigation();
  setConnection("offline");
  if (hub.ready) {
    if (location.hash !== "#login") location.hash = "login";
    else showLogin();
  }
}

function acceptIdentity(identity) {
  hub.user = identity.user;
  hub.csrf = identity.csrf_token;
  hub.isBootstrap = identity.is_bootstrap === true;
  renderNavigation();
}

function renderNavigation() {
  const nav = $("#workspace-nav");
  nav.replaceChildren();
  $("#pi-navigation").classList.toggle("hidden", !hub.user || !hub.isBootstrap);
  if (!hub.user) return;
  const routes = [["jobs", "Jobs"], ["usage", "Usage"], ["tunnels", "Tunnels"], ["marimo", "Marimo"], ["keys", "API keys"], ["account", "Account"]];
  if (hub.user.is_admin) routes.push(["admin/users", "Users"]);
  for (const [path, label] of routes) {
    const link = node("a", "workspace-link", label);
    link.href = `#${path}`;
    if (hub.route === path) link.setAttribute("aria-current", "page");
    nav.append(link);
  }
  nav.append(button("Log out", async () => {
    try { await api("/api/v1/auth/logout", { method: "POST" }); }
    finally { clearIdentity(); }
  }));
}

function showLogin() {
  for (const view of [listView, globalView, detailView]) view.classList.add("hidden");
  workspaceView.classList.remove("hidden");
  workspaceContent.replaceChildren();
  notice("");
  title.textContent = "Sign in";
  subtitle.textContent = "Worker Harness";
  const panel = section("Sign in");
  const feedback = node("p", "notice", "");
  feedback.setAttribute("role", "status");
  const form = formBox("", async (current) => {
    const data = values(current);
    try {
      const identity = await api("/api/v1/auth/login", { method: "POST", body: JSON.stringify(data) });
      password.value = "";
      acceptIdentity(identity);
      location.hash = "jobs";
    } catch (error) {
      password.value = "";
      notice(error.status === 429 ? "Too many attempts. Wait a minute before trying again." : error.message, true);
    }
  }, "Sign in");
  field(form, "Username", "username", { required: true, autocomplete: "username", maxLength: 64 });
  const password = field(form, "Password", "password", { type: "password", required: true, autocomplete: "current-password" });
  // Login must accept legacy input long enough to return the same generic failure.
  password.removeAttribute("minlength");
  panel.append(form, feedback);
}

async function boot() {
  try { acceptIdentity(await api("/api/v1/auth/me")); }
  catch (error) { if (error.status !== 401) notice(`Unable to restore your account: ${error.message}`, true); }
  hub.ready = true;
  await route();
}

function modal(heading) {
  const dialog = node("dialog", "workspace-dialog");
  dialog.append(node("h2", "", heading));
  const message = node("p", "notice");
  message.setAttribute("role", "status");
  dialog.append(message);
  const close = () => { dialog.close(); dialog.remove(); hub.dialogs.delete(dialog); };
  dialog.append(button("Close", close));
  dialog.addEventListener("close", () => { dialog.remove(); hub.dialogs.delete(dialog); });
  document.body.append(dialog);
  hub.dialogs.add(dialog);
  dialog.showModal();
  return { dialog, message, close };
}

function delayEntries(entries = []) {
  const result = node("div", "delay-entries");
  for (const entry of entries) {
    if (entry.delay_seconds != null && entry.delay_seconds <= 0) continue;
    const details = node("details", "delay-entry");
    details.append(node("summary", "", entry.delay_seconds == null
      ? "Moved later; delay estimate unavailable"
      : `+${Math.ceil(entry.delay_seconds / 60)} minutes estimated delay because of ${entry.moved_job_name} by ${entry.moved_job_owner_display_name}`));
    details.append(node("p", "", `Moved by ${entry.actor_display_name}`), node("p", "", entry.reason), node("p", "muted", when(entry.created_at)));
    result.append(details);
  }
  return result;
}

function jobStatus(job) {
  return job.execution_state === "termination_unknown" ? "termination_unknown — allocation retained"
    : job.execution_state === "stopping" || job.stop_requested_at ? "stopping — awaiting confirmed termination" : job.status;
}
function priorityFields(form, job = {}) {
  const priority = field(form, "Priority (does not change FIFO)", "priority", {
    value: job.priority || "normal", choices: ["flexible", "normal", "blocking", "time_critical"].map((key) => [key, key.replaceAll("_", " ")]),
  });
  const note = field(form, "Public priority note", "priority_note", { type: "textarea", value: job.priority_note || "", maxLength: 1000 });
  const update = () => { note.required = priority.value === "time_critical"; };
  priority.addEventListener("change", update);
  update();
  let local = "";
  if (job.deadline_at != null) {
    const date = new Date(job.deadline_at * 1000);
    local = new Date(date.getTime() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
  }
  field(form, "Deadline (optional, local time)", "deadline_at", { type: "datetime-local", value: local });
}
function metadata(data) {
  return {
    expected_seconds: Number(data.expected_seconds), priority: data.priority,
    priority_note: data.priority_note.trim(), deadline_at: data.deadline_at ? Math.floor(new Date(data.deadline_at).getTime() / 1000) : null,
  };
}
function durationFields(form, seconds = 0) {
  const amount = field(form, "Expected duration", "duration", { type: "number", min: 0.01, required: true, value: seconds > 0 ? seconds / 60 : "" });
  amount.step = "any";
  field(form, "Duration unit", "duration_unit", { value: "60", choices: [["60", "Minutes"], ["3600", "Hours"]] });
}
function durationSeconds(data) {
  const seconds = Math.round(Number(data.duration) * Number(data.duration_unit));
  if (!(seconds > 0)) throw new Error("Expected duration must be positive.");
  return seconds;
}

function jobActions(job, worker) {
  const actions = node("div", "row-actions");
  if (job.owner_user_id === hub.user.id) {
    const link = node("a", "action-button", "Details / logs");
    link.href = `#job/${enc(job.id)}`;
    actions.append(link);
    if (["pending", "starting", "running"].includes(job.status)) {
      actions.append(button("Edit metadata", () => editJob(job)));
      actions.append(button("Stop", async () => {
        if (!confirm(`Stop "${job.name}"?`)) return;
        try { await api(`/api/v1/jobs/${enc(job.id)}`, { method: "DELETE" }); }
        catch (error) {
          if (error.status === 503) notice("Stop unconfirmed. The allocation is retained until termination is confirmed.", true);
          throw error;
        }
        await refreshVisible();
      }, "danger"));
    }
  }
  if (job.status === "pending" && worker) actions.append(button("Move…", () => moveJob(job, worker)));
  return actions;
}

function editJob(job) {
  const { dialog, message, close } = modal(`Edit ${job.name}`);
  const pending = job.status === "pending";
  const form = formBox("", async (current) => {
    const data = values(current);
    const body = metadata({ ...data, expected_seconds: durationSeconds(data) });
    if (pending) Object.assign(body, { name: data.name.trim(), worker_id: data.worker_id, gpu_count: Number(data.gpu_count) });
    try {
      await api(`/api/v1/jobs/${enc(job.id)}${pending ? "/queue" : ""}`, { method: "PATCH", body: JSON.stringify(body) });
      close();
      await refreshVisible();
    } catch (error) { message.textContent = error.message; }
  }, "Save metadata");
  if (pending) {
    field(form, "Public name", "name", { value: job.name, required: true });
    workerField(form, job.worker_id);
    field(form, "GPU count", "gpu_count", { type: "number", value: job.gpu_count, required: true, min: 1 });
  }
  durationFields(form, job.expected_seconds);
  priorityFields(form, job);
  dialog.append(form);
}

function moveJob(job, initialWorker) {
  const { dialog, message } = modal(`Move ${job.name}`);
  let worker = initialWorker;
  let preview = null;
  const result = node("div");
  const destinations = (current) => [["", "To end"], ...current.jobs
    .filter((row) => row.status === "pending" && row.id !== job.id)
    .map((row) => [row.id, `Before ${row.name} — ${row.owner_display_name}`])];
  // Every preview is computed against the queue as it is now: a 409 or a
  // concurrent change never leaves the dialog holding a stale revision.
  async function refreshWorker() {
    const board = await api(`/api/v1/jobs/queue?worker_id=${enc(worker.worker_id)}`);
    const fresh = board.workers.find((item) => item.worker_id === worker.worker_id);
    if (!fresh || !fresh.jobs.some((row) => row.id === job.id && row.status === "pending")) {
      throw new Error("This job is no longer pending; nothing to move.");
    }
    worker = fresh;
    const select = form.elements.before_job_id;
    const selected = select.value;
    select.replaceChildren(...destinations(worker).map(([value, label]) => {
      const option = node("option", "", label);
      option.value = value;
      return option;
    }));
    select.value = destinations(worker).some(([value]) => value === selected) ? selected : "";
  }
  const form = formBox("", async (current) => {
    preview = null;
    result.replaceChildren();
    const data = values(current);
    const reason = data.reason.trim();
    if (!reason) { message.textContent = "A reason is required."; return; }
    try {
      const target = data.before_job_id || null;
      await refreshWorker();
      if (target && form.elements.before_job_id.value !== target) {
        message.textContent = "The chosen destination is no longer pending. Choose again and preview.";
        return;
      }
      const response = await api(`/api/v1/jobs/${enc(job.id)}/queue/preview`, {
        method: "POST", body: JSON.stringify({ before_job_id: target, expected_revision: worker.revision, reason }),
      });
      if (!response.changed) { message.textContent = "No change"; return; }
      preview = response;
      message.textContent = `Estimate at move; preview expires ${when(response.expires_at)}.`;
      result.append(impactTable(response.impacts));
      result.append(button("Apply move", async () => {
        if (!preview) return;
        try {
          await api(`/api/v1/jobs/${enc(job.id)}/queue/move`, {
            method: "POST", body: JSON.stringify({ preview_id: preview.preview_id, expected_revision: preview.revision }),
          });
          preview = null;
          result.replaceChildren();
          message.textContent = "Move applied.";
          await refreshVisible();
        } catch (error) { await conflict(error); }
      }, "primary"));
    } catch (error) { await conflict(error); }
  }, "Preview");
  async function conflict(error) {
    preview = null;
    result.replaceChildren();
    if (error.status === 409) {
      message.textContent = "The queue changed or the preview expired. Review the refreshed destinations and preview again.";
      await refreshVisible();
      try { await refreshWorker(); } catch (refreshError) { message.textContent = refreshError.message; }
    } else message.textContent = error.message;
  }
  field(form, "Destination", "before_job_id", { choices: destinations(worker) });
  field(form, "Public reason", "reason", { type: "textarea", required: true, maxLength: 1000 });
  form.addEventListener("input", () => { preview = null; result.replaceChildren(); message.textContent = "Preview the updated move before applying."; });
  dialog.append(form, result);
}

function impactTable(impacts) {
  return table(["Job / owner", "Before position", "After position", "Before start", "After start", "Estimated delay"], impacts.map((impact) => [
    `${impact.name} / ${impact.owner_display_name}`, impact.before_position, impact.after_position,
    when(impact.before_start_at), when(impact.after_start_at),
    impact.delay_seconds == null ? "unavailable" : `${impact.delay_seconds > 0 ? "+" : ""}${impact.delay_seconds} seconds`,
  ]));
}

function queueRows(jobs, worker) {
  if (!jobs.length) return node("p", "muted", "No jobs.");
  return table(["Position / state", "Public job / owner", "GPUs", "Priority / deadline", "Expected runtime", "Estimated start / finish", "Actions"], jobs.map((job) => {
    const identity = node("div");
    identity.append(node("strong", "", job.name), node("p", "muted", job.owner_display_name), delayEntries(job.delay_history));
    const priority = node("div", "", job.priority.replaceAll("_", " "));
    if (job.priority_note) priority.append(node("p", "", job.priority_note));
    if (job.deadline_at) priority.append(node("p", "muted", `Deadline ${when(job.deadline_at)}`));
    const estimate = node("div");
    if (job.estimate_status === "known") estimate.append(node("p", "", `Start ${when(job.estimated_start_at)}`), node("p", "", `Finish ${when(job.estimated_finish_at)}`));
    else estimate.append(node("p", "unknown-estimate", `Unknown: ${job.estimate_explanation || "No reliable estimate available."}`));
    return [`${job.position || "Active"} · ${jobStatus(job)}`, identity, job.gpu_count, priority, runtime(job.expected_seconds), estimate, jobActions(job, worker)];
  }));
}

function mountJobs() {
  const create = section("Submit a job");
  create.append(node("p", "muted", "Names, priority notes, deadlines, and move reasons are public. Commands and logs are private. Priority and deadlines never change FIFO."));
  const form = formBox("", async (current) => {
    const data = values(current);
    const body = {
      worker_id: data.worker_id, name: data.name.trim(), command: data.command,
      ...metadata({ ...data, expected_seconds: durationSeconds(data) }),
    };
    if (data.mode === "gpu") body.gpu_count = Number(data.gpu_count);
    await api(`/api/v1/jobs${data.mode === "gpu" ? "/queue" : ""}`, { method: "POST", body: JSON.stringify(body) });
    notice("Job submitted.");
    await refreshVisible();
  }, "Submit job");
  const mode = field(form, "Mode", "mode", { value: "cpu", choices: [["cpu", "CPU setup (immediate)"], ["gpu", "GPU (FIFO queue)"]] });
  workerField(form);
  field(form, "Public name", "name", { required: true });
  field(form, "Command (private)", "command", { type: "textarea", required: true });
  durationFields(form);
  const gpu = field(form, "GPU count", "gpu_count", { type: "number", min: 1, value: 1 });
  const updateMode = () => {
    gpu.closest("label").classList.toggle("hidden", mode.value !== "gpu");
    gpu.disabled = mode.value !== "gpu";
    gpu.required = mode.value === "gpu";
  };
  mode.addEventListener("change", updateMode);
  updateMode();
  priorityFields(form);
  create.append(form);
  const boardSection = section("Shared FIFO job board");
  const filterForm = node("div", "form-grid");
  const filter = workerField(filterForm, "", true);
  boardSection.append(filterForm);
  const board = node("div");
  empty(board, "Loading queue…");
  boardSection.append(board);
  const history = section("Your completed / failed jobs");
  const ownRows = node("div");
  history.append(ownRows);
  const events = section("Queue history — estimates at move, not actual delay");
  const eventRows = node("div");
  events.append(eventRows);
  let nextEvent = null;
  const more = button("Older events", async () => {
    if (!nextEvent) return;
    const params = new URLSearchParams({ before_event_id: nextEvent, limit: "50" });
    if (filter.value) params.set("worker_id", filter.value);
    const response = await api(`/api/v1/jobs/queue/history?${params}`);
    renderEvents(eventRows, response.events, true);
    nextEvent = response.next_before_event_id;
    more.hidden = !nextEvent;
  });
  events.append(more);
  const generation = hub.routeGeneration;
  hub.refresh = async () => {
    const query = filter.value ? `?worker_id=${enc(filter.value)}` : "";
    const [response, owned, historyResponse] = await Promise.all([
      api(`/api/v1/jobs/queue${query}`), api(`/api/v1/jobs${query}`),
      api(`/api/v1/jobs/queue/history${query ? `${query}&` : "?"}limit=50`),
    ]);
    if (generation !== hub.routeGeneration) return;
    hub.board = response;
    board.replaceChildren();
    for (const worker of response.workers) {
      const group = section(`${worker.worker_name} · revision ${worker.revision}`, board);
      group.append(node("h3", "", "Active GPU work"), queueRows(worker.jobs.filter((job) => job.status !== "pending"), worker));
      group.append(node("h3", "", "Pending GPU work (FIFO)"), queueRows(worker.jobs.filter((job) => job.status === "pending"), worker));
      group.append(node("h3", "", "Active CPU / setup (no queue slots)"), queueRows(worker.cpu_jobs, worker));
    }
    if (!response.workers.length) empty(board, "No workers available.");
    const completed = owned.filter((job) => ["done", "failed"].includes(job.status));
    ownRows.replaceChildren(completed.length ? table(["Job", "State", "Termination reason", "Finished", "Details"], completed.map((job) => [job.name, jobStatus(job), job.termination_reason, when(job.finished_at), jobActions(job)])) : node("p", "muted", "No completed jobs."));
    renderEvents(eventRows, historyResponse.events);
    nextEvent = historyResponse.next_before_event_id;
    more.hidden = !nextEvent;
  };
  filter.addEventListener("change", () => void refreshVisible());
}

function renderEvents(target, events, append = false) {
  if (!append) target.replaceChildren();
  if (!events.length && !append) { empty(target, "No queue history."); return; }
  for (const event of events) {
    const details = node("details", "history-event");
    const verb = event.event_type === "reorder" ? "Moved by" : event.event_type === "worker_move" ? "Moved to another worker by" : "Updated by";
    details.append(node("summary", "", `${event.event_type.replaceAll("_", " ")} · ${event.job_name} · ${verb} ${event.actor_display_name} · ${when(event.created_at)}`));
    details.append(node("p", "", `Job owner: ${event.job_owner_display_name}`), node("p", "", event.reason), node("p", "muted", `Revision ${event.old_revision} → ${event.new_revision}; estimate at move`), impactTable(event.impacts));
    target.append(details);
  }
}

function mountJob(id) {
  const info = section("Your private job detail");
  const detail = node("div");
  info.append(detail);
  const logSection = section("Private logs");
  const controls = node("div", "form-grid");
  const tail = field(controls, "Tail lines", "tail", { type: "number", min: 1, max: 10000, value: 200 });
  const follow = field(controls, "Follow logs (refresh every five seconds)", "follow", { type: "checkbox", value: true });
  const logs = node("pre", "job-logs", "Loading logs…");
  logSection.append(controls, logs);
  let first = true;
  const generation = hub.routeGeneration;
  hub.refresh = async () => {
    const job = await api(`/api/v1/jobs/${enc(id)}`);
    if (generation !== hub.routeGeneration) return;
    detail.replaceChildren(node("h3", "", job.name), node("p", "", `${job.worker_name} · ${jobStatus(job)}`), node("p", "", `Termination reason: ${job.termination_reason || "none"}`), node("p", "", `Expected runtime: ${runtime(job.expected_seconds)}; priority: ${job.priority}`), node("pre", "job-command", job.command), jobActions(job), delayEntries(job.delay_history));
    if (first || follow.checked) {
      const response = await api(`/api/v1/jobs/${enc(id)}/logs?tail=${Math.max(1, Math.min(10000, Number(tail.value) || 200))}`);
      if (generation !== hub.routeGeneration) return;
      logs.textContent = response.logs || "No log output yet.";
      first = false;
    }
  };
  controls.append(button("Load logs", async () => { first = true; await refreshVisible(); }));
}

function showIssuedKey(parent, token) {
  if (!parent.isConnected || !hub.user) return;
  clearTokens();
  const container = node("div", "issued-key");
  container.append(node("p", "warning", "Copy this key now. It is shown only once and is discarded when you leave this view."));
  const input = node("input");
  input.value = token;
  input.readOnly = true;
  input.autocomplete = "off";
  input.setAttribute("aria-label", "New API key, shown once");
  hub.tokens.add(input);
  const feedback = node("span", "muted");
  container.append(input, button("Copy", async () => {
    try { await copyText(input.value); feedback.textContent = "Copied"; }
    catch (error) { feedback.textContent = error.message; }
  }), feedback, button("Dismiss key", clearTokens));
  parent.append(container);
}
function keyRows(keys, base, reload) {
  if (!keys.length) return node("p", "muted", "No API keys.");
  return table(["Label", "Prefix", "Created", "Last used", "Revoked", "Action"], keys.map((key) => [
    key.label, key.prefix, when(key.created_at), when(key.last_used_at), key.revoked_at ? when(key.revoked_at) : "Active",
    key.revoked_at ? "—" : button("Revoke", async () => {
      if (!confirm(`Revoke key "${key.label}"?`)) return;
      await api(`${base}/${enc(key.id)}`, { method: "DELETE" });
      await reload();
    }, "danger"),
  ]));
}
function mountKeys() {
  const panel = section("Your API keys");
  panel.append(node("p", "muted", "Keys grant ordinary job, tunnel, and Marimo access. Administration requires a browser session."));
  const issued = node("div");
  const rows = node("div");
  const form = formBox("Create API key", async (current) => {
    const response = await api("/api/v1/auth/keys", { method: "POST", body: JSON.stringify(values(current)) });
    showIssuedKey(issued, response.token);
    current.reset();
    await refreshVisible();
  });
  field(form, "Label", "label", { required: true, maxLength: 80 });
  panel.append(form, issued, rows);
  const generation = hub.routeGeneration;
  hub.refresh = async () => {
    const keys = await api("/api/v1/auth/keys");
    if (generation === hub.routeGeneration) rows.replaceChildren(keyRows(keys, "/api/v1/auth/keys", refreshVisible));
  };
}

function mountUsage() {
  const panel = section("Usage by user and worker");
  panel.append(node("p", "warning", "Allocated time, not utilization or quota. GPU hours are allocated GPU-seconds; CPU job hours are wall time, not CPU core-hours."));
  const rows = node("div", "", "Loading usage…");
  panel.append(rows);
  const generation = hub.routeGeneration;
  hub.refresh = async () => {
    const response = await api("/api/v1/usage");
    if (generation !== hub.routeGeneration) return;
    const hours = (seconds) => (seconds / 3600).toFixed(2);
    rows.replaceChildren(node("p", "muted", `As of ${when(response.as_of)}${response.provisional ? " · provisional" : ""}`));
    rows.append(response.rows.length ? table(["User", "Worker", "Allocated GPU hours", "CPU job hours", "Job hours", "Running", "Accounting"], response.rows.map((row) => [
      row.owner_display_name, row.worker_name || row.worker_id, hours(row.allocated_gpu_seconds), hours(row.cpu_job_seconds), hours(row.job_seconds), row.running_jobs,
      accountingBadges(row),
    ])) : node("p", "muted", "No allocated job time yet."));
  };
}

function accountingBadges(row) {
  const badges = node("div");
  if (row.provisional) badges.append(node("span", "account-badge", "Provisional"));
  if (row.historical_estimate) badges.append(node("span", "account-badge", "Historical estimate"));
  if (!badges.childElementCount) badges.textContent = "Confirmed";
  return badges;
}

function mountTunnels() {
  const panel = section("Your generic tunnels");
  panel.append(node("p", "warning", "Raw TCP tunnels are reachable by anyone on the trusted Tailnet. The target service must authenticate its users. Use managed Marimo for protected notebooks."));
  const form = formBox("Create tunnel", async (current) => {
    const data = values(current);
    await api("/api/v1/tunnels", { method: "POST", body: JSON.stringify({ ...data, local_port: Number(data.local_port), remote_port: Number(data.remote_port) }) });
    notice("Tunnel created.");
    await refreshVisible();
  });
  workerField(form);
  field(form, "Local port", "local_port", { type: "number", min: 1024, max: 65535, required: true });
  field(form, "Worker loopback port", "remote_port", { type: "number", min: 1, max: 65535, required: true });
  field(form, "Name", "name");
  const rows = node("div", "", "Loading tunnels…");
  panel.append(form, rows);
  const generation = hub.routeGeneration;
  hub.refresh = async () => {
    const tunnels = await api("/api/v1/tunnels");
    if (generation !== hub.routeGeneration) return;
    rows.replaceChildren(tunnels.length ? table(["Name", "Worker", "Listener", "Worker port", "Created", "Action"], tunnels.map((tunnel) => [
      tunnel.service_name || tunnel.id, tunnel.worker_name, `${tunnel.bind_host}:${tunnel.local_port}`, tunnel.remote_port, when(tunnel.created_at),
      button("Remove", async () => {
        if (!confirm(`Remove tunnel "${tunnel.service_name || tunnel.id}"?`)) return;
        try { await api(`/api/v1/tunnels/${enc(tunnel.id)}`, { method: "DELETE" }); }
        catch (error) {
          if (error.code === "MANAGED_BY_MARIMO") throw new Error("This forward is managed by Marimo. Stop the managed notebook instead.");
          throw error;
        }
        await refreshVisible();
      }, "danger"),
    ])) : node("p", "muted", "No tunnels."));
  };
}

function mountMarimo() {
  const panel = section("Your managed Marimo notebooks");
  panel.append(node("p", "muted", "Managed sessions are CPU-only and use an owner-authenticated proxy. For GPU notebooks, submit an ordinary FIFO GPU command."));
  const form = formBox("Start notebook", async (current) => {
    const data = values(current);
    notice("Starting notebook; waiting for readiness…");
    await api("/api/v1/marimo", { method: "POST", body: JSON.stringify({ ...data, ready_timeout: Number(data.ready_timeout) }) });
    notice("Notebook ready.");
    await refreshVisible();
  }, "Start notebook");
  workerField(form);
  field(form, "Notebook path", "notebook_path", { required: true });
  field(form, "Environment", "environment", { required: true });
  field(form, "Ready timeout (seconds)", "ready_timeout", { type: "number", min: 1, required: true, value: 30 });
  const rows = node("div", "", "Loading notebooks…");
  panel.append(form, rows);
  const generation = hub.routeGeneration;
  hub.refresh = async () => {
    const sessions = await api("/api/v1/marimo");
    if (generation !== hub.routeGeneration) return;
    rows.replaceChildren(sessions.length ? table(["Notebook", "Worker", "Environment", "Status", "Actions"], sessions.map((session) => {
      const actions = node("div", "row-actions");
      if (session.status === "ready") {
        const open = node("a", "action-button", "Open");
        try {
          const url = new URL(session.url, location.origin);
          if (url.origin !== location.origin || url.pathname !== `/api/v1/marimo/${enc(session.id)}/proxy/`) throw new Error("Invalid notebook link");
          open.href = url.href;
          open.target = "_blank";
          open.rel = "noopener noreferrer";
          actions.append(open);
        } catch { actions.append(node("span", "warning", "Notebook link unavailable")); }
        actions.append(button("Stop", async () => {
          if (!confirm(`Stop notebook "${session.notebook_path}"?`)) return;
          try { await api(`/api/v1/marimo/${enc(session.id)}`, { method: "DELETE" }); }
          catch (error) {
            if (error.status === 503) throw new Error("Stop unconfirmed. Notebook resources are retained until termination can be confirmed; retry after checking its state.");
            throw error;
          }
          await refreshVisible();
        }, "danger"));
      }
      actions.append(button("Refresh details", async () => {
        const detail = await api(`/api/v1/marimo/${enc(session.id)}`);
        notice(`${detail.notebook_path}: ${detail.status}`);
        await refreshVisible();
      }));
      return [session.notebook_path, session.worker_name, session.environment, session.status, actions];
    })) : node("p", "muted", "No managed notebooks."));
  };
}

function mountAccount() {
  const panel = section("Change password");
  panel.append(node("p", "muted", "Changing your password signs out every browser session. API keys are managed separately."));
  const form = formBox("", async (current) => {
    await api("/api/v1/auth/password", { method: "POST", body: JSON.stringify(values(current)) });
    current.reset();
    clearIdentity();
  }, "Change password and sign out");
  field(form, "Current password", "current_password", { type: "password", required: true, autocomplete: "current-password" });
  field(form, "New password (12–1024 characters)", "new_password", { type: "password", required: true, autocomplete: "new-password" });
  panel.append(form);
}

function mountAdmin() {
  const panel = section("User administration");
  const issued = node("div");
  const create = formBox("Create user", async (current) => {
    const data = values(current);
    await api("/api/v1/admin/users", { method: "POST", body: JSON.stringify({
      username: data.username.toLowerCase(), display_name: data.display_name, password: data.password, is_admin: data.is_admin === "on",
    }) });
    current.reset();
    notice("User created.");
    await refreshVisible();
  });
  const username = field(create, "Username", "username", { required: true, min: 3, maxLength: 64, autocomplete: "off" });
  username.pattern = "[a-zA-Z0-9][a-zA-Z0-9_-]{2,63}";
  field(create, "Display name", "display_name", { required: true, maxLength: 100 });
  field(create, "Initial password", "password", { type: "password", required: true, autocomplete: "new-password" });
  field(create, "Administrator", "is_admin", { type: "checkbox" });
  const rows = node("div", "", "Loading users…");
  panel.append(create, issued, rows);
  const generation = hub.routeGeneration;
  // User editors are opened separately so polling never destroys a password draft.
  hub.refresh = async () => {
    const users = await api("/api/v1/admin/users");
    if (generation !== hub.routeGeneration) return;
    rows.replaceChildren(table(["Username", "Display name", "Role", "State", "Actions"], users.map((user) => {
      const actions = node("div", "row-actions");
      const base = `/api/v1/admin/users/${enc(user.id)}`;
      async function patch(body) {
        const updated = await api(base, { method: "PATCH", body: JSON.stringify(body) });
        if (updated.id === hub.user.id) { hub.user = updated; renderNavigation(); }
        await refreshVisible();
      }
      actions.append(button("Rename", () => {
        const editor = modal(`Rename ${user.username}`);
        const form = formBox("", async (current) => {
          try { await patch(values(current)); editor.close(); }
          catch (error) { editor.message.textContent = error.message; }
        }, "Save");
        field(form, "Display name", "display_name", { value: user.display_name, required: true, maxLength: 100 });
        editor.dialog.append(form);
      }));
      actions.append(button(user.disabled ? "Re-enable" : "Disable", async () => {
        if (confirm(`${user.disabled ? "Re-enable" : "Disable"} ${user.username}? Disabling revokes credentials and cancels pending jobs, not running work.`)) await patch({ disabled: !user.disabled });
      }));
      actions.append(button(user.is_admin ? "Remove admin" : "Make admin", async () => {
        if (confirm(`Change administrator status for ${user.username}?`)) await patch({ is_admin: !user.is_admin });
      }));
      actions.append(button("Reset password", () => {
        const editor = modal(`Reset password: ${user.username}`);
        const form = formBox("", async (current) => {
          try {
            await api(`${base}/password`, { method: "POST", body: JSON.stringify(values(current)) });
            current.reset();
            editor.close();
            if (user.id === hub.user.id) clearIdentity();
            else notice("Password reset; browser sessions revoked.");
          } catch (error) { editor.message.textContent = error.message; }
        }, "Reset password");
        field(form, "New password", "password", { type: "password", required: true, autocomplete: "new-password" });
        editor.dialog.append(form);
      }));
      actions.append(button("Manage keys", () => adminKeys(user, base)));
      return [user.username, user.display_name, user.is_admin ? "Administrator" : "User", user.disabled ? "Disabled" : "Enabled", actions];
    })));
  };
}

async function adminKeys(user, base) {
  const editor = modal(`API keys: ${user.display_name}`);
  const issued = node("div");
  const rows = node("div", "", "Loading keys…");
  const keysBase = `${base}/keys`;
  const reload = async () => {
    const keys = await api(keysBase);
    if (editor.dialog.isConnected) rows.replaceChildren(keyRows(keys, keysBase, reload));
  };
  const create = formBox("Issue ordinary API key", async (current) => {
    try {
      const response = await api(keysBase, { method: "POST", body: JSON.stringify(values(current)) });
      showIssuedKey(issued, response.token);
      current.reset();
      await reload();
    } catch (error) { editor.message.textContent = error.message; }
  });
  field(create, "Label", "label", { required: true, maxLength: 80 });
  editor.dialog.append(create, issued, rows);
  editor.dialog.addEventListener("close", clearTokens);
  try { await reload(); } catch (error) { editor.message.textContent = error.message; }
}

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
  const generation = hub.identityGeneration;
  const controller = new AbortController();
  hub.requests.add(controller);
  const method = (options.method || "GET").toUpperCase();
  try {
    const response = await fetch(path, {
      ...options, credentials: "same-origin", cache: "no-store", signal: controller.signal,
      headers: {
        "content-type": "application/json",
        ...(!["GET", "HEAD"].includes(method) && hub.csrf ? { "X-CSRF-Token": hub.csrf } : {}),
        ...(options.headers || {}),
      },
    });
    if (!response.ok) {
      let detail;
      try { detail = (await response.json()).detail; } catch { /* non-JSON error */ }
      const error = new Error(typeof detail === "string" ? detail : detail?.message || (Array.isArray(detail) ? "Invalid input. Check the form fields." : `${response.status} ${response.statusText}`));
      error.status = response.status;
      error.code = detail?.code;
      if (response.status === 401) clearIdentity();
      throw error;
    }
    const result = response.status === 204 ? null : await response.json();
    if (generation !== hub.identityGeneration) throw new DOMException("Identity changed", "AbortError");
    return result;
  } finally { hub.requests.delete(controller); }
}

async function loadSessions({ quiet = false } = {}) {
  if (!hub.user || !hub.isBootstrap || document.hidden) return false;
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
  if (!hub.user || !hub.isBootstrap || document.hidden) return;
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
  if (!hub.user || !hub.isBootstrap) return;
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
  if (!hub.user || !hub.isBootstrap || document.hidden) return;
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
    target.replaceChildren(purifier.sanitize(parsed, {
      USE_PROFILES: { html: true }, RETURN_DOM_FRAGMENT: true,
      FORBID_TAGS: ["style", "img", "form", "input", "textarea", "select", "button", "iframe", "object", "embed"],
    }));
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


async function route() {
  if (!hub.ready) return;
  const generation = ++hub.routeGeneration;
  stopPolling();
  closeDialogs();
  clearTokens();
  closeStream();
  closeGlobal();
  state.navigation += 1;
  state.selected = null;
  for (const view of [listView, globalView, detailView, workspaceView]) view.classList.add("hidden");
  document.body.classList.remove("session-open", "global-open");
  setSidebarOpen(false);
  hub.refresh = null;
  hub.route = location.hash.slice(1) || "jobs";
  if (!hub.user) {
    if (hub.route !== "login") { location.hash = "login"; return; }
    showLogin();
    return;
  }
  if (hub.route === "login") { location.hash = "jobs"; return; }
  renderNavigation();
  if (hub.route === "global" || hub.route.startsWith("session/")) {
    if (!hub.isBootstrap) { location.hash = "jobs"; return; }
    $("#pi-navigation").classList.remove("hidden");
    await loadSessions();
    if (generation !== hub.routeGeneration) return;
    if (hub.route === "global") await openGlobal();
    else await openSession(decodeURIComponent(hub.route.slice(8)));
    hub.refresh = async () => {
      await api("/api/v1/auth/me");
      await loadSessions({ quiet: true });
      if (state.selected && !state.source) connectStream(state.selected.id);
    };
  } else {
    workspaceView.classList.remove("hidden");
    workspaceContent.replaceChildren();
    notice("");
    title.textContent = ({ jobs: "Job board", keys: "API keys", usage: "Usage", tunnels: "Tunnels", marimo: "Marimo", "admin/users": "Users", account: "Account" })[hub.route] || "Job detail";
    subtitle.textContent = `${hub.user.display_name} · Worker Harness`;
    try {
      if (["jobs", "tunnels", "marimo"].includes(hub.route) || hub.route.startsWith("job/")) {
        hub.workers = await api("/api/v1/workers");
        if (generation !== hub.routeGeneration) return;
      }
      if (hub.route === "jobs") mountJobs();
      else if (hub.route.startsWith("job/")) mountJob(decodeURIComponent(hub.route.slice(4)));
      else if (hub.route === "keys") mountKeys();
      else if (hub.route === "usage") mountUsage();
      else if (hub.route === "tunnels") mountTunnels();
      else if (hub.route === "marimo") mountMarimo();
      else if (hub.route === "account") mountAccount();
      else if (hub.route === "admin/users" && hub.user.is_admin) mountAdmin();
      else { location.hash = "jobs"; return; }
      await refreshVisible();
    } catch (error) { if (generation === hub.routeGeneration && hub.user) notice(error.message, true); }
  }
  if (generation === hub.routeGeneration && hub.user) startPolling();
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
refreshButton.addEventListener("click", () => void refreshVisible());
window.addEventListener("hashchange", () => void route());
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
document.addEventListener("visibilitychange", () => {
  if (document.hidden) { stopPolling(); closeStream(); closeGlobalSources(); }
  else if (hub.user) { void refreshVisible(); startPolling(); }
});
await boot();
