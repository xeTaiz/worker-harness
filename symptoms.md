# Symptoms: `wh_dispatch exec` Failure on Worker Harness Fleet

This document is a **symptom-only** report. It contains no analysis of root cause. Independent diagnosis is welcome.

## 1. Initial report

Starting **2026-08-17 ~13:01 UTC**, `wh_dispatch exec` calls began failing for **all six online workers** simultaneously. Sync and async dispatch paths both affected. HTTP heartbeats, Pi interactive sessions, and the web UI continued to work normally.

User description: "many other agents that keep getting this -1 exit code also" and "this was not an issue just an hour ago, but now suddenly it is."

## 2. Harness-level failure pattern

Every `wh_dispatch exec` (sync or async) on every online worker produces one of:
- A job that completes in **zero seconds** (`started_at == finished_at`)
- `exit_code: -1` (harness internal sentinel for "no exit code captured")

The signature was identical across all workers and across all command shapes (single `echo`, multi-stage pipelines, GPU probes, `nvidia-smi -L`, git pull + verify, etc.). Exit code 1 / 2 / 7 were also seen for some commands, but those corresponded to legitimate command-level failures within the harness's normal job lifecycle; the dominant pattern was the `-1` zero-duration failure.

## 3. Cutover timing (worker-by-worker)

| Worker                       | Last successful job   | First zero-duration failure | Gap                  |
| ---------------------------- | --------------------- | --------------------------- | --------------------- |
| KW60898                      | 13:00:52 UTC          | 13:01:10 UTC                | 18 seconds            |
| KW60995.kaust.edu.sa         | 12:37:47 UTC          | 13:00:57 UTC                | 23 min (idle before)  |
| KW61627                      | 12:55:10 UTC          | 14:09:40 UTC                | later (other failures)|
| KW60996                      | (later in window)     | later                       | -                     |

The tightest window (KW60898, 18 seconds) places the cutover at **2026-08-17 13:01:00 UTC**.

## 4. Workers affected

All 6 online workers failed identically:
- `KW60898` (100.64.2.54)
- `KW60995.kaust.edu.sa` (100.64.0.103)
- `KW60996` (100.64.0.10)
- `KW61627` (100.64.2.84)
- `KW61633` (100.64.2.83)
- `gpu210-02` (100.64.2.89)

Worker `archdome` (100.64.0.2) was already offline (last seen ~2 days prior) and unaffected.

## 5. What continued to work

- HTTP worker heartbeats (worker → orchestrator, port 12888)
- Pi interactive sessions on `KW60898` and `KL-27469` (Tailscale Pi relay, port 27888)
- Web UI
- Existing tunnel: tensorboard tunnel `8c1affa0-...` on KW60995 stayed up

## 6. Architecture (verified via harness data and Tailscale)

- The Mac (`engeld@mac`, Tailscale `100.64.0.3`) is **not** the orchestrator.
- The harness's HTTP client default (` `~/.pi/agent/extensions/pi-worker-harness/api.ts`):
  ```ts
  let orchestratorUrl = process.env.WH_ORCHESTRATOR_URL?.trim()
    || "http://orchestrator.hs.d0me.xyz:12889";
  ```
- The orchestrator is a **separate Linux host** on Tailscale:
  ```
  100.64.2.55   orchestrator  tagged-devices  linux  active; direct 46.38.245.109:18122
  ```
- SSH from the Mac to `orchestrator.hs.d0me.xyz:22` is **refused** (`Connection refused`). This is normal — sshd is not exposed on the orchestrator and the harness path does not use SSH to the orchestrator.
- The Mac only runs `host-relay.ts` (Bun process, PID 6263, started `Wed Jul 29 10:39:04 2026` — 19 days uptime at time of investigation).

## 7. User observations on the Mac (raw, recorded for completeness)

User checked several things on the Mac. None of these affect the harness, but they were specifically noted by the user:

- `~/.ssh/authorized_keys` does **not exist** on the Mac.
- `~/.ssh/` is a real directory (not a symlink). Contents: `id_ed25519`, `id_ed25519.pub`, `config`, `known_hosts`, `known_hosts.old`, `agent/`.
  - `id_ed25519` and `id_ed25519.pub` last modified `Apr 2 10:21:41 2025`.
  - `config` and `known_hosts` last modified `Aug 16 07:30:27 2026` / `Aug 16 07:30:32 2026` (≈30 hours before cutover).
- `~/.pi -> dotfiles/pi/.pi` and `~/.omp -> dotfiles/oh-my-pi/.omp` are real stow-style symlinks (not managed by GNU Stow directly, but the same idea).
- `~/dotfiles` git log shows **no commits touching `.ssh/` or any path that would propagate to the orchestrator**. The most recent commit at time of cutover was:
  - `5d7fedc 2026-08-17 11:30:08 +0300 add omp and pi mise + sandbox wrapper local bins`
  - 4.5 hours **before** the cutover.
- OMP rule files at `~/.omp/agent/rules/wh-over-ssh.md` and `wh-over-scp.md` modified at 12:41 UTC (20 minutes before cutover). Content:
  ```
  You were about to use SSH into a remote machine. Stop.
  To run commands on the Worker Harness fleet use the wh_dispatch tool.
  Only ever run `ssh` when EXPLICITLY asked by user.
  ```
- `~/.pi/agent/settings.json` has uncommitted local modifications vs the dotfiles copy (diff is unrelated to SSH).
- No `~/.pi/sessions/` or `~/.omp/sessions/` directories exist.
- `crontab -l`: `no crontab for engeld`.
- `~/Library/LaunchAgents/`: only `skhd`, `yabai`, `mailspring` plists — none related.
- The Mac has been up 104 days, 22 hours, 55 minutes with no restart.

## 8. Orchestrator logs (provided by user)

```
wh-orch    | 2026-08-17 14:16:47,606 heartbeat-server INFO Worker registered/updated: KW61633 (id=bc3ea2b0-8b14-4a36-b43f-89cc344f927f, ip=100.64.2.83)
wh-orch    | 2026/08/17 14:16:49 magicsock: new contact: peer=[WFsDo] usec=346497810021 cached=false via=derp
wh-orch    | 2026-08-17 14:16:54,010 job-manager ERROR Failed to start job c9298fb1-499c-4644-b3e6-860675386aca on KW60995.kaust.edu.sa: runtime: failed to create new OS thread (have 7 already; errno=11)
wh-orch    | runtime: may need to increase max user processes (ulimit -u)
wh-orch    | fatal error: newosproc
wh-orch    |
wh-orch    | runtime stack:
wh-orch    | runtime.throw({0x108e35f?, 0x1afce7997dd0?})
wh-orch    |    runtime/panic.go:1229 +0x48 fp=0x1afce7997da8 sp=0x1afce7997d78 pc=0x48cde8
wh-orch    | runtime.newosproc(0x1afce7c00008)
wh-orch    |    runtime/os_linux.go:199 +0x165 fp=0x1afce7997e18 sp=0x1afce7997d98 pc=0x44f7c5
wh-orch    | runtime.newm1(0x1afce7c00008)
wh-orch    |    runtime/proc.go:2927 +0xbf fp=0x1afce7997e58 sp=0x1afce7997d98 pc=0x45a61f
wh-orch    | runtime.newm(0x49612d?, 0x1afce78e8008, 0x656311709a5a3a?)
wh-orch    |    runtime/proc.go:2902 +0x125 fp=0x1afce7997e88 sp=0x1afce7997d98 pc=0x45a4e5
wh-orch    | runtime.startm(0x1afce78e8008?, 0x1, 0x0)
wh-orch    |    runtime/proc.go:3096 +0x130 fp=0x1afce7997ed8 sp=0x1afce7997e98 pc=0x48d0b0
wh-orch    | runtime.wakep()
wh-orch    |    runtime/proc.go:3243 +0xec fp=0x1afce7997f00 sp=0x1afce7997e88 pc=0x45dab0
wh-orch    | runtime.resetspinning()
wh-orch    |    runtime/proc.go:4034 +0x3e fp=0x1afce7997f20 sp=0x1afce7997f00 pc=0x45d11e
wh-orch    | runtime.schedule()
wh-orch    |    runtime/proc.go:4199 +0x127 fp=0x1afce7997f60 sp=0x1afce7997f20 pc=0x45d647
wh-orch    | runtime.park_m(0x1afce7984780)
wh-orch    |    runtime/proc.go:4304 +0x285 fp=0x1afce7997fc0 sp=0x1afce7997f60 pc=0x45da65
wh-orch    | runtime.mcall()
wh-orch    |    runtime/asm_amd64.s:496 +0x55 fp=0x1afce7997fd8 sp=0x1afce7997fc0 pc=0x492f15
wh-orch    |
wh-orch    | goroutine 1 gp=0x1afce78841e0 m=nil [select, locked to thread]:
wh-orch    | runtime.gopark(0x1afce7992fe8?, 0x5?, 0x0?, 0xac?, 0x1afce7992e8e?)
wh-orch    |    runtime/proc.go:462 +0xce fp=0x1afce7992d20 sp=0x1afce7997d00 pc=0x48cf0e
wh-orch    | runtime.selectgo(0x1afce7992fe8, 0x1afce7992e84, 0x1094fc5?, 0x0, 0x1afce799a3f0?, 0x1)
wh-orch    |    runtime/select.go:351 +0xaa5 fp=0x1afce7992d20 sp=0x1afce7997e50 pc=0x4694e5
wh-orch    | net/http.(*persistConn).roundTrip(0x1afce7922000, 0x1afce7ad6690)
wh-orch    |    net/http/transport.go:2917 +0x83d fp=0x1afce7993080 sp=0x1afce7992e50 pc=0x7f567d
wh-orch    | net/http.(*Transport).roundTrip(0x1afce7a95d40, 0x1afce7ab0a00)
wh-orch    |    net/http/transport.go:710 +0xbca fp=0x1afce7993288 sp=0x1afce7993080 pc=0x7e922a
wh-orch    | net/http.(*Transport).RoundTrip(0x500fc00000001?, 0x112b460?)
wh-orch    |    net/http/roundtrip.go:33 +0x18 fp=0x1afce79932a8 sp=0x1afce7993288 pc=0x7f8f98
wh-orch    | net/http.send(0x1afce7ab0a00, {0x112b460, 0x1afce7993520?, 0x48fc86?, 0x0?})
wh-orch    |    net/http/client.go:264 +0x64b fp=0x1afce7993498 sp=0x1afce79932a8 pc=0x78606b
wh-orch    | net/http.(*Client).send(0x1afce7abf740, 0x1afce7ab0a00, {0x301a3ce5f?, 0x424a66?, 0x0?})
wh-orch    |    net/http/client.go:185 +0x258 fp=0x1afce7993530 sp=0x1afce7993498 pc=0x7858b8
wh-orch    | net/http.(*Client).do(0x1afce7abf740, 0x1afce7ab0a00)
wh-orch    |    net/http/client.go:733 +0x9d7 fp=0x1afce7993720 sp=0x1afce7993538 pc=0x787e57
wh-orch    | net/http.(*Client).Do(...)
wh-orch    |    net/http/client.go:592
wh-orch    | tailscale.com/client/local.(*Client).DoLocalRequest(0x1a19000, 0x1afce7ab0a00)
wh-orch    |    tailscale.com@v1.102.2/client/local/local.go:161 +0x167 fp=0x1afce7993798 sp=0x1afce7993720 pc=0x99c027
wh-orch    | tailscale.com/client/local.(*Client).doLocalRequestNiceError(0x1137930?, 0x1afce7ab0a00)
wh-orch    |    tailscale.com@v1.102.2/client/local/local.go:165 +0x27 fp=0x1afce7993868 fp=0x1afce7993868 sp=0x1afce7993798 pc=0x99c227
wh-orch    | tailscale.com/client/local.(*Client).sendWithHeaders(0x1a19000, {0x1137930, 0x1a3b2c0}, {0x1087fe8, 0x3}, {0x109a65e?, 0x1afce79939e0?}, 0xc8, {0x0, 0x0}, ...)
wh-orch    |    tailscale.com@v1.102.2/client/local/local.go:281 +0xda fp=0x1afce7993998 fp=0x1afce7993868 pc=0x99ce5a
wh-orch    | tailscale.com/client/local.(*Client).send(0x1a19000, {0x1137930, 0x1afce7993a10 pc=0x99cc13})
wh-orch    | tailscale.com/client/local.(*Client).get200(...)
wh-orch    |    tailscale.com@v1.102.2/client/local/local.go:304
wh-orch    | tailscale.com/client/local.(*Client).status(0x1a19000, {0x1137930, 0x1a3b2c0}, {0x0?, 0x1?})
wh-orch    |    tailscale.com@v1.102.2/client/local/local.go:784 +0x85 fp=0x1afce7993a90 sp=0x1afce7993a10 pc=0x9a03a5
wh-orch    | tailscale.com/client/local.(*Client).Status(...)
wh-orch    |    tailscale.com@v1.102.2/client/local/local.go:768
wh-orch    | tailscale.com/cmd/tailscale/cli.runSSH({0x1137930, 0x1a3b2c0}, {0x1afce7abed60?, 0x2, 0x1afce7abf680?})
wh-orch    |    tailscale.com@v1.102.2/cmd/tailscale/cli/ssh.go:64 +0xda fp=0x1afce7993c70 fp=0x1afce7993a90 pc=0xd7f0da
…
wh-orch    | main.main()
wh-orch    |    tailscale.com@v1.102.2/cmd/tailscale/tailscale.go:22 +0xf1 fp=0x1afce7993f48 fp=0x1afce7993ee8 pc=0xdacd71
```

Key facts in the log:

- The panic is in the **Go runtime** of the **`tailscale` CLI v1.102.2** (`tailscale.com/cmd/tailscale/cli/runSSH`, called from `cli.Run`), not in the orchestrator's own Python process.
- Triggering call: `runtime.newosproc` at `runtime/os_linux.go:199` — this is `clone(2)` returning `EAGAIN` (errno=11).
- The panic message reports "**have 7 already**" — i.e. the Go process was trying to create an 8th OS thread (M).
- `magicsock: new contact: peer=[WFsDo] usec=346497810021 cached=false via=derp` — Tailscale daemon post-cutover activity showing peer-reconnection churn.
- `heartbeat-server INFO Worker registered/updated: KW61633` — orchestrator still receiving and registering worker heartbeats during the failure window.

## 9. Recovery

The user **restarted the orchestrator container**, then verified:
- `pgrep 'tailscale ssh'` on the orchestrator host returns **0 matches**.
- `wh_dispatch exec` was dispatched and completed successfully on KW60898, KW60995, KW60996, KW61627 with `exit_code: 0` and full command output (hostname, `uname -a`, `nvidia-smi -L`).

User's stated hypothesis after restart: "I assume there is a leakage of tailscale ssh zombies. … zombie processes stacked up since it was running for several days and many execs were performed, potentially building up to a limit."

## 10. Harness jobs observed during the failure window (representative)

| Job id (short)         | Worker              | tmux_session                       | started    | finished   | exit | command |
|                                 |                     |                                  |            |            | code |                    |
| --------------------------- | ------------------- | ---------------------------------- | ---------- | ---------- | ---- | ------------------------------------------------------------------- |
| `f69066d0-…` (probe)         | KW60995.kaust.edu.sa | wh_test-exec-functionality        | 1786975657 | 1786975657 | -1   | `echo "wh_dispatch exec working" && hostname && date && uname -a && nvidia-smi -L` |
| `252bfe49-…` (probe)         | KW60995.kaust.edu.sa | wh_test-echo                      | 1786975677 | 1786975678 | -1   | `echo hello`                                                          |
| `853e15b0-…` (probe)         | KW60995.kaust.edu.sa | wh_probe-kw60995                  | 1786975692 | 1786975692 | -1   | `echo hello; date; whoami; uptime`                                  |
| `5dc8526c-…`                 | KW60995.kaust.edu.sa | wh_alive-check-3                  | 1786974663 | 1786974663 | -1   | `echo alive-check-3 && date`                                         |
| `aab69b7e-…`                 | KW60995.kaust.edu.sa | wh_alive-check-10                 | 1786976136 | 1786976136 | -1   | `echo alive-check-10 && date && hostname`                           |
| `b18ac00a-…`                 | KW60995.kaust.edu.sa | wh_alive-check-8                  | 1786976013 | 1786976013 | -1   | `echo alive-check-8 && date`                                         |
| `a0958b26-…`                 | KW60995.kaust.edu.sa | wh_pull-and-verify-fix            | 1786975961 | 1786975962 | -1   | `cd /code/lc64-radfoam && git fetch origin face-continuity-v1 && git pull origin face-continuity-v1 && git log --oneline -3 && ls experiments/face-continuity_v1/push_split_slices.py` |
| `aab6f81e-…` (KW60898 success) | KW60898              | wh_62032ee8-check-eval-config-…  | 1786970889 | 1786970892 | 0    | (config parity check)                                                |
| `bd0cf85a-…` (KW60995 post-restart) | KW60995.kaust.edu.sa | wh_post-restart-test-kw60995 | (post-    | (post-     | 0    | `echo HELLO-KW60995; hostname; uname -a; date`                       |
|                                 |                     |                                  | restart)  | restart)   |      |                                                                      |
| `46be3cce-…` (KW60898 post-restart) | KW60898           | wh_post-restart-test-kw60898     | (post-    | (post-     | 0    | `echo HELLO-KW60898; hostname; uname -a; date; nvidia-smi -L`         |
|                                 |                     |                                  | restart)  | restart)   |      |                                                                      |

(All pre-restart jobs above have `started_at == finished_at` and `exit_code: -1`. All post-restart jobs ran to completion with `exit_code: 0`.)

## 11. Relevant source files (read-only references)

- Orchestrator Python source: `~/Dev/worker-harness/src/worker_harness/ssh.py` (467 lines)
- Orchestrator Python source: `~/Dev/worker-harness/src/worker_harness/tunnel_registry.py`
- Orchestrator Python source: `~/Dev/worker-harness/src/worker_harness/heartbeat.py`
- Orchestrator Python source: `~/Dev/worker-harness/src/worker_harness/orchestrator.py`
- Orchestrator container: `~/Dev/worker-harness/orchestrator_container/Dockerfile`
- Orchestrator container: `~/Dev/worker-harness/orchestrator_container/entrypoint.sh`
- Worker harness extension on the Mac: `~/.pi/agent/extensions/pi-worker-harness/`