#!/usr/bin/env bun

import { readFile, realpath } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const [action, packageRootArg, cwdArg, sessionIdArg] = process.argv.slice(2);

function fail(message) {
  process.stderr.write(`${message}\n`);
  process.exit(1);
}

function compatibleVersion(version) {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/.exec(String(version));
  if (!match) return false;
  const [, major, minor] = match.map(Number);
  return major === 0 && minor >= 83;
}

if (!['list', 'resolve'].includes(action)) fail('history helper action must be list or resolve');
if (!packageRootArg || !cwdArg || !isAbsolute(cwdArg)) fail('package root and absolute cwd are required');
if (action === 'resolve' && (!sessionIdArg || /[\0\r\n]/.test(sessionIdArg))) {
  fail('an exact session ID is required');
}

const packageRoot = await realpath(packageRootArg).catch(() => fail('Pi package root does not exist'));
const manifest = JSON.parse(await readFile(join(packageRoot, 'package.json'), 'utf8'));
if (!['@earendil-works/pi-coding-agent', '@mariozechner/pi-coding-agent'].includes(manifest.name)) {
  fail(`unsupported Pi package ${String(manifest.name)}`);
}
if (!compatibleVersion(manifest.version)) {
  fail(`Pi ${String(manifest.version)} is unsupported; Worker Harness history resume requires >=0.83.0,<1.0.0`);
}

const entry = join(packageRoot, 'dist', 'index.js');
const imported = await import(pathToFileURL(entry).href);
if (!imported.SessionManager || typeof imported.SessionManager.list !== 'function') {
  fail('installed Pi does not export SessionManager.list()');
}

const cwd = resolve(cwdArg);
const sessionRoot = await realpath(join(homedir(), '.pi', 'agent', 'sessions')).catch(() => null);
const rows = await imported.SessionManager.list(cwd);
const safe = [];
for (const row of rows) {
  if (!row || typeof row.id !== 'string' || !row.id || typeof row.path !== 'string') continue;
  const path = await realpath(row.path).catch(() => null);
  if (!path || !sessionRoot) continue;
  const fromRoot = relative(sessionRoot, path);
  if (!fromRoot || fromRoot.startsWith('..') || isAbsolute(fromRoot)) continue;
  if (resolve(String(row.cwd || '')) !== cwd) continue;
  safe.push({
    id: row.id,
    name: typeof row.name === 'string' ? row.name : '',
    cwd,
    created_at: row.created instanceof Date ? row.created.toISOString() : String(row.created || ''),
    modified_at: row.modified instanceof Date ? row.modified.toISOString() : String(row.modified || ''),
    message_count: Number(row.messageCount || 0),
    first_message: String(row.firstMessage || '').slice(0, 500),
  });
}
safe.sort((a, b) => b.modified_at.localeCompare(a.modified_at));

if (action === 'resolve') {
  const exact = safe.filter((row) => row.id === sessionIdArg);
  if (exact.length !== 1) fail('exact Pi session ID was not found under the requested cwd');
  process.stdout.write(`${JSON.stringify(exact[0])}\n`);
} else {
  process.stdout.write(`${JSON.stringify(safe.slice(0, 200))}\n`);
}
