# Follow-up Migration Plan: `zerotier_ip` -> `overlay_ip`

## Goal

Rename network-address fields to VPN-agnostic naming while preserving compatibility with existing databases and API clients.

## Phase 1 (compatibility window)

- Keep current DB column `workers.zerotier_ip`.
- Keep current API field `zerotier_ip`.
- Store Tailscale IP in that field (already in place).
- Add code comments + docs clarifying transitional semantics.

## Phase 2 (dual-read/write)

1. DB schema migration:
   - Add new nullable column:
     - `ALTER TABLE workers ADD COLUMN overlay_ip TEXT;`
   - Backfill:
     - `UPDATE workers SET overlay_ip = zerotier_ip WHERE overlay_ip IS NULL;`

2. Code updates:
   - Models:
     - add `overlay_ip` field
     - keep `zerotier_ip` as deprecated alias during transition
   - DB layer:
     - write both columns
     - read `overlay_ip` first, fallback to `zerotier_ip`
   - SSH layer:
     - use `overlay_ip` (with fallback)

3. API transition:
   - Registration endpoint accepts either `overlay_ip` or `zerotier_ip`.
   - Responses include `overlay_ip` and optionally deprecated `zerotier_ip`.

## Phase 3 (deprecation removal)

- After one release window:
  - remove writes to `zerotier_ip`
  - remove `zerotier_ip` from response payloads
  - remove fallback logic

## Validation checklist

- Existing DB opens without manual migration.
- Existing workers sending `zerotier_ip` still register correctly.
- New workers sending `overlay_ip` register correctly.
- Orchestrator SSH operations continue to target the correct worker address.
- CLI/TUI display remains correct across mixed old/new records.
