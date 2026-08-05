"""``wh host`` subcommands: capture and validate the isolated host-runtime manifest."""

from __future__ import annotations

import json
import stat as _stat
from pathlib import Path
from typing import Sequence

import typer
from rich.console import Console
from rich.table import Table

from worker_harness.host_runtime import (
    HostRuntime,
    HostRuntimeError,
    OPTIONAL_EXECUTABLES,
    REQUIRED_EXECUTABLES,
    capture_host_runtime,
    default_manifest_path,
    load_host_runtime,
    validate_host_runtime,
    validation_failures,
    validation_warnings,
    write_host_runtime,
)

app = typer.Typer(help="Capture and validate the isolated host-runtime manifest", no_args_is_help=True)
console = Console()


def _output_mode() -> str:
    from worker_harness.cli.app import _state

    return _state.get("output", "text")


def _emit_json(payload: object) -> None:
    """Print a JSON payload without Rich's terminal-width wrapping."""

    text = json.dumps(payload, indent=2)
    console.print(text, soft_wrap=True, overflow="ignore", crop=False)


def _format_mode(mode: int) -> str:
    return f"{mode & 0o777:04o}"


def _check_parent_mode(path: Path) -> tuple[bool, str]:
    if not path.parent.exists():
        return False, f"parent directory missing: {path.parent}"
    try:
        parent_stat = path.parent.stat()
    except OSError as exc:
        return False, f"parent directory not statable: {exc}"
    if not _stat.S_ISDIR(parent_stat.st_mode):
        return False, f"parent is not a directory: {path.parent}"
    parent_mode = parent_stat.st_mode & 0o777
    if parent_mode != 0o700:
        return False, (
            f"parent directory mode is {_format_mode(parent_mode)}; expected 0700: {path.parent}"
        )
    return True, "ok"


def _check_file_mode(path: Path) -> tuple[bool, str]:
    if not path.exists():
        return False, f"manifest missing: {path}"
    try:
        file_stat = path.stat()
    except OSError as exc:
        return False, f"manifest not statable: {exc}"
    if not _stat.S_ISREG(file_stat.st_mode):
        return False, f"manifest is not a regular file: {path}"
    file_mode = file_stat.st_mode & 0o777
    if file_mode != 0o600:
        return False, (
            f"manifest file mode is {_format_mode(file_mode)}; expected 0600: {path}"
        )
    return True, "ok"


def _path_errors(runtime: HostRuntime) -> list[str]:
    return [entry for entry in runtime.path if not Path(entry).is_dir()]


def _required_status_payload(
    runtime: HostRuntime,
    manifest_path: Path,
    parent_ok: bool,
    parent_detail: str,
    file_ok: bool,
    file_detail: str,
    probes: dict,
) -> dict:
    failures = validation_failures(probes)
    warnings = validation_warnings(probes)
    path_errors = _path_errors(runtime)
    return {
        "manifest": str(manifest_path),
        "schema_version": runtime.schema_version,
        "generated_at": runtime.generated_at,
        "path": list(runtime.path),
        "executables": dict(runtime.executables),
        "parent_ok": parent_ok,
        "parent_detail": parent_detail,
        "file_ok": file_ok,
        "file_detail": file_detail,
        "probes": {name: probe.to_dict() for name, probe in probes.items()},
        "failures": [probe.to_dict() for probe in failures],
        "warnings": [probe.to_dict() for probe in warnings],
        "missing_path_directories": path_errors,
        "ok": bool(parent_ok and file_ok and not failures and not path_errors),
    }


@app.command("setup")
def setup_cmd(
    output: str | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Override root output format: text or json",
    ),
) -> None:
    """Capture, validate, and atomically write the host-runtime manifest."""

    from worker_harness.cli.app import _state

    if output is not None:
        _state["output"] = output
    manifest_path = default_manifest_path()
    text_mode = _output_mode() != "json"
    try:
        runtime = capture_host_runtime()
    except HostRuntimeError as exc:
        message = f"host runtime capture failed: {exc}"
        if text_mode:
            console.print(f"[red]{message}[/]")
        else:
            _emit_json({"ok": False, "error": message})
        raise typer.Exit(1) from exc
    try:
        probes = validate_host_runtime(runtime)
    except HostRuntimeError as exc:
        message = f"host runtime validation failed before write: {exc}"
        if text_mode:
            console.print(f"[red]{message}[/]")
        else:
            _emit_json({"ok": False, "error": message})
        raise typer.Exit(1) from exc
    failures = validation_failures(probes)
    if failures:
        summary = ", ".join(f"{check.name} ({check.error or 'exit'} {check.exit_code})" for check in failures)
        message = f"host runtime validation failed: {summary}"
        if text_mode:
            console.print(f"[red]{message}[/]")
        else:
            _emit_json({
                "ok": False,
                "error": message,
                "failures": [probe.to_dict() for probe in failures],
            })
        raise typer.Exit(1) from HostRuntimeError(summary)
    try:
        written_path = write_host_runtime(runtime)
    except HostRuntimeError as exc:
        message = f"host runtime write failed: {exc}"
        if text_mode:
            console.print(f"[red]{message}[/]")
        else:
            _emit_json({"ok": False, "error": message})
        raise typer.Exit(1) from exc
    payload = {
        "ok": True,
        "manifest": str(written_path),
        "schema_version": runtime.schema_version,
        "generated_at": runtime.generated_at,
        "path": list(runtime.path),
        "executables": dict(runtime.executables),
        "probes": {name: probe.to_dict() for name, probe in probes.items()},
        "warnings": [probe.to_dict() for probe in validation_warnings(probes)],
    }
    if text_mode:
        console.print(f"[green]Wrote host runtime manifest[/] [dim]{written_path}[/]")
        console.print(f"  schema_version: {runtime.schema_version}")
        console.print(f"  generated_at:   {runtime.generated_at}")
        console.print("  executables:")
        for name in REQUIRED_EXECUTABLES:
            console.print(f"    {name:<10} {runtime.executable(name)}")
        for name in OPTIONAL_EXECUTABLES:
            value = runtime.executable(name)
            console.print(f"    {name:<10} {value or '<absent>'}")
        console.print("  PATH:")
        for entry in runtime.path:
            console.print(f"    {entry}")
        warnings = payload["warnings"]
        if warnings:
            console.print("[yellow]Warnings:[/]")
            for warning in warnings:
                console.print(f"  {warning['name']}: {warning.get('error') or 'nonzero exit'}")
    else:
        _emit_json(payload)


@app.command("doctor")
def doctor_cmd(
    output: str | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Override root output format: text or json",
    ),
) -> None:
    """Load and verify the existing host-runtime manifest."""

    from worker_harness.cli.app import _state

    if output is not None:
        _state["output"] = output
    manifest_path = default_manifest_path()
    text_mode = _output_mode() != "json"
    if not manifest_path.exists():
        message = f"host runtime manifest missing: {manifest_path}; run `wh host setup`"
        if text_mode:
            console.print(f"[red]{message}[/]")
        else:
            _emit_json({"ok": False, "error": message})
        raise typer.Exit(1) from HostRuntimeError(message)
    try:
        runtime = load_host_runtime(required=True)
    except HostRuntimeError as exc:
        message = f"host runtime manifest invalid: {exc}"
        if text_mode:
            console.print(f"[red]{message}[/]")
        else:
            _emit_json({"ok": False, "error": message})
        raise typer.Exit(1) from exc
    if runtime is None:
        message = f"host runtime manifest missing: {manifest_path}; run `wh host setup`"
        if text_mode:
            console.print(f"[red]{message}[/]")
        else:
            _emit_json({"ok": False, "error": message})
        raise typer.Exit(1) from HostRuntimeError(message)
    parent_ok, parent_detail = _check_parent_mode(manifest_path)
    file_ok, file_detail = _check_file_mode(manifest_path)
    probes = validate_host_runtime(runtime)
    failures = validation_failures(probes)
    warnings = validation_warnings(probes)
    path_errors = _path_errors(runtime)
    payload = _required_status_payload(
        runtime=runtime,
        manifest_path=manifest_path,
        parent_ok=parent_ok,
        parent_detail=parent_detail,
        file_ok=file_ok,
        file_detail=file_detail,
        probes=probes,
    )
    if text_mode:
        console.print(f"manifest: {manifest_path}")
        console.print(f"  schema_version: {runtime.schema_version}")
        console.print(f"  generated_at:   {runtime.generated_at}")
        console.print(f"  parent:         {parent_detail}")
        console.print(f"  file mode:      {file_detail}")
        console.print("  probes:")
        table = Table(show_header=True, header_style="bold")
        table.add_column("tool")
        table.add_column("required")
        table.add_column("status")
        table.add_column("exit")
        table.add_column("detail")
        all_probes: Sequence[tuple[str, object]] = [
            (name, probes.get(name)) for name in REQUIRED_EXECUTABLES
        ]
        for name in OPTIONAL_EXECUTABLES:
            if name in probes:
                all_probes.append((name, probes[name]))
        for name, probe in all_probes:
            if probe is None:
                table.add_row(name, "-", "skipped", "-", "absent")
                continue
            status = "ok" if probe.ok else ("timeout" if probe.timed_out else "fail")
            detail = probe.error or (probe.output.splitlines()[-1] if probe.output else "")
            table.add_row(
                name,
                "yes" if probe.required else "no",
                status,
                "-" if probe.exit_code is None else str(probe.exit_code),
                detail[:60],
            )
        console.print(table)
        if failures:
            console.print("[red]Failures:[/]")
            for probe in failures:
                console.print(f"  {probe.name}: {probe.error or f'exit {probe.exit_code}'}")
        if warnings:
            console.print("[yellow]Warnings:[/]")
            for probe in warnings:
                console.print(f"  {probe.name}: {probe.error or f'exit {probe.exit_code}'}")
        if path_errors:
            console.print("[red]Missing PATH directories:[/]")
            for entry in path_errors:
                console.print(f"  {entry}")
        if not (parent_ok and file_ok and not failures and not path_errors):
            console.print(f"[red]host runtime doctor FAILED[/] ({manifest_path})")
        else:
            console.print(f"[green]host runtime doctor OK[/] ({manifest_path})")
    else:
        _emit_json(payload)
    if not (parent_ok and file_ok and not failures and not path_errors):
        raise typer.Exit(1)


__all__ = ["app", "setup_cmd", "doctor_cmd"]