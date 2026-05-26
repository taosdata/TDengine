#!/usr/bin/env python3
"""Render a safe dry-run gdb plan and optional command file."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
from pathlib import Path
from typing import List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a dry-run plan and optional .gdb command file for single-thread debugging."
    )
    parser.add_argument("--binary", help="Path to the target binary.")
    parser.add_argument("--core", help="Path to a core file.")
    parser.add_argument("--pid", type=int, help="PID to attach to. Generates a plan only; does not attach.")
    parser.add_argument("--workdir", help="Working directory for the debug session.")
    parser.add_argument(
        "--breakpoint",
        action="append",
        default=[],
        help="Breakpoint hint such as main, file.c:42, or func if cond.",
    )
    parser.add_argument(
        "--command-file",
        help="Write gdb commands to this file instead of printing only the dry-run summary.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format for the dry-run summary.",
    )
    parser.add_argument(
        "program_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed after '--' are treated as inferior program arguments.",
    )
    args = parser.parse_args()

    if not (args.binary or args.pid):
        parser.error("provide --binary for run/core mode or --pid for attach mode")
    if args.core and not args.binary:
        parser.error("--core requires --binary so gdb can load symbols")
    if args.pid and args.core:
        parser.error("--pid and --core are mutually exclusive")
    return args


def infer_mode(args: argparse.Namespace) -> str:
    if args.pid is not None:
        return "attach"
    if args.core:
        return "core"
    return "run"


def normalize_program_args(raw: List[str]) -> List[str]:
    if raw and raw[0] == "--":
        return raw[1:]
    return raw


def choose_breakpoints(mode: str, hints: List[str]) -> List[str]:
    cleaned = [bp.strip() for bp in hints if bp and bp.strip()]
    if cleaned:
        return cleaned
    if mode == "run":
        return ["main"]
    return []


def build_commands(args: argparse.Namespace, mode: str, breakpoints: List[str]) -> List[str]:
    commands = [
        "set pagination off",
        "set breakpoint pending on",
        "set print pretty on",
        "set print elements 0",
    ]

    if args.binary:
        commands.append(f"file {shell_quote_for_gdb(args.binary)}")
    if args.workdir:
        commands.append(f"cd {shell_quote_for_gdb(args.workdir)}")
    if args.core:
        commands.append(f"core-file {shell_quote_for_gdb(args.core)}")
    if args.pid is not None:
        commands.append(f"attach {args.pid}")
    if mode == "run":
        program_args = normalize_program_args(args.program_args)
        if program_args:
            commands.append("set args " + " ".join(shell_quote_for_gdb(arg) for arg in program_args))
    for bp in breakpoints:
        commands.append(f"break {bp}")
    if mode == "run":
        commands.append("run")

    commands.extend(
        [
            "info threads",
            "bt full",
            "frame 0",
            "info args",
            "info locals",
            "list",
        ]
    )
    return commands


def shell_quote_for_gdb(value: str) -> str:
    # GDB accepts shell-like quoting for these commands; shlex.quote is conservative and readable.
    return shlex.quote(value)


def build_entry_command(args: argparse.Namespace, mode: str) -> str:
    pieces = ["gdb", "-q"]
    if mode == "run":
        pieces.append("--args")
        pieces.append(args.binary)
        pieces.extend(normalize_program_args(args.program_args))
    elif mode == "core":
        pieces.extend([args.binary, args.core])
    else:
        if args.binary:
            pieces.append(args.binary)
        pieces.append(str(args.pid))
    return shlex.join(pieces)


def write_command_file(path: str, commands: List[str]) -> None:
    Path(path).write_text("\n".join(commands) + "\n", encoding="utf-8")


def build_summary(args: argparse.Namespace, mode: str, breakpoints: List[str], commands: List[str]) -> dict:
    risks = []
    if mode == "attach":
        risks.append("live attach pauses the target process")
    if mode == "run":
        risks.append("run mode starts or restarts the target process")
    if not breakpoints:
        risks.append("no explicit breakpoint hints were provided")

    return {
        "mode": mode,
        "binary": args.binary,
        "binary_exists": bool(args.binary and Path(args.binary).exists()),
        "core": args.core,
        "core_exists": bool(args.core and Path(args.core).exists()),
        "pid": args.pid,
        "workdir": args.workdir or os.getcwd(),
        "program_args": normalize_program_args(args.program_args),
        "breakpoints": breakpoints,
        "entry_command": build_entry_command(args, mode),
        "command_count": len(commands),
        "risks": risks,
        "readonly_first_pass": [
            "info threads",
            "bt full",
            "frame 0",
            "info args",
            "info locals",
            "list",
        ],
    }


def render_text(summary: dict, commands: List[str], command_file: str | None) -> str:
    lines = [
        f"Mode: {summary['mode']}",
        f"Binary: {summary['binary'] or '(none)'}",
        f"Core: {summary['core'] or '(none)'}",
        f"PID: {summary['pid'] if summary['pid'] is not None else '(none)'}",
        f"Workdir: {summary['workdir']}",
        "Program args: " + (shlex.join(summary["program_args"]) if summary["program_args"] else "(none)"),
        "Breakpoints: " + (", ".join(summary["breakpoints"]) if summary["breakpoints"] else "(none)"),
        f"Entry command: {summary['entry_command']}",
        "Risks:",
    ]
    if summary["risks"]:
        lines.extend(f"- {risk}" for risk in summary["risks"])
    else:
        lines.append("- no extra risks detected")
    if command_file:
        lines.append(f"Command file: {command_file}")
    lines.append("GDB commands:")
    lines.extend(f"{idx + 1:02d}. {cmd}" for idx, cmd in enumerate(commands))
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    mode = infer_mode(args)
    breakpoints = choose_breakpoints(mode, args.breakpoint)
    commands = build_commands(args, mode, breakpoints)

    if args.command_file:
        write_command_file(args.command_file, commands)

    summary = build_summary(args, mode, breakpoints, commands)
    if args.format == "json":
        json.dump({"summary": summary, "commands": commands}, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(render_text(summary, commands, args.command_file) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
