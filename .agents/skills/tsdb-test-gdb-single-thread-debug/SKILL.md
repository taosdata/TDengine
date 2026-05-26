---
name: tsdb-test-gdb-single-thread-debug
description: Debug single-threaded native programs with GDB, starting from optional user breakpoint hints and ending with evidence-backed suspect code locations. Use when Codex needs to investigate a crash, hang, wrong result, assertion failure, or corrupted state in C, C++, Rust, or other native code with gdb, core files, or a local single-thread reproduction.
metadata:
  author: Tony Zhang
  owner_team: engine
  version: 1.0.0
---

# GDB Single-Thread Debug

## Overview

Use GDB to investigate a single-threaded native program and report the code locations most likely causing the bug. Start from the user's symptom and optional breakpoint hints, prefer low-risk inspection, and end with a short evidence-backed debug report.

## Inputs And Outputs

Accept these inputs when available:
- target binary path and runtime arguments
- core file path, or PID when live attach is unavoidable
- optional breakpoint hints from the user; treat them as starting points, not ground truth
- symptom summary: crash, hang, bad return value, corrupted state, assertion, or infinite loop
- recent logs, stack traces, and expected behavior

Produce these outputs:
- suspect code locations as `path:line`, function names, or both
- the evidence that points to each location: stack, locals, arguments, watched state, or invariant break
- confidence level and remaining uncertainty
- next breakpoint or watchpoint to try if the issue is not fully pinned down

If you cannot narrow the issue to a code location, say so explicitly and report the deepest verified boundary instead of guessing.

## Safety Defaults

Treat these as high-risk actions:
- attaching to a live PID
- continuing or restarting a production process
- sending signals to the inferior
- `call`, `print` with side-effectful expressions, `set variable`, or writing memory/registers
- deleting or mutating crash artifacts

For any high-risk action:
1. explain the concrete risk and likely impact
2. ask for explicit confirmation
3. offer a dry-run first, including the exact command or command file

Prefer this order of safety:
1. inspect an existing core file
2. reproduce locally under gdb
3. attach to a live process only when the first two are not available

If `info threads` shows more than one thread, stop and say this skill no longer fits cleanly. Either isolate a single-thread reproduction or switch to a multi-thread debugging approach.

## Workflow

### 1. Triage The Debug Target

Collect the minimal facts needed to debug:
- binary path, args, cwd, and symbol availability
- whether the symptom is a crash, hang, or wrong-result bug
- whether you have a core file, reproducible local run, or only a live PID
- any user-suggested breakpoints

If symbols are missing, still inspect the coarse stack, then recommend rebuilding with debug symbols such as `-g` and reduced optimization when possible.

### 2. Build A Dry-Run Plan

Use `scripts/render_gdb_plan.py` to turn the target and optional breakpoint hints into a dry-run summary and a reusable `.gdb` command file.

Use the script before running gdb when:
- you want a predictable starting script
- the user provided several breakpoint hints
- you may need to show the plan for confirmation first

Run examples:

```bash
python3 scripts/render_gdb_plan.py \
  --binary ./build/bin/app \
  --breakpoint main \
  --breakpoint src/parser.c:128 \
  --command-file /tmp/app-debug.gdb \
  -- --config conf/app.toml
```

```bash
python3 scripts/render_gdb_plan.py \
  --binary ./build/bin/app \
  --core ./core.12345 \
  --command-file /tmp/app-core.gdb
```

If the user gave no breakpoint hints:
- for `run` mode, seed with `main` or the nearest known failure boundary
- for `core` mode, start from the crashing frame and caller chain
- for `attach` mode, interrupt once, inspect the current frame, then place narrower breakpoints

### 3. Run A Read-Only First Pass

Prefer read-only inspection commands first:
- `info threads`
- `bt` or `bt full`
- `frame N`
- `info args`
- `info locals`
- `list`
- `print EXPR`
- `x/FMT ADDRESS`

Use entry modes like this:
- local reproduction: `gdb -q --args BIN ...`
- core file: `gdb -q BIN CORE`
- live attach after confirmation: `gdb -q BIN PID`

For a hang or infinite loop, interrupt execution once and inspect the stopped frame before adding new breakpoints.

### 4. Narrow The Fault

Move from broad evidence to narrow evidence:
- inspect the crashing frame, then callers, then the first owner of bad state
- add conditional breakpoints near state transitions, not every line
- use watchpoints only for a small number of critical variables or addresses
- prefer `next` over `step` when the current function is still trustworthy
- switch to `step` when ownership, parsing, bounds checks, or memory writes become suspicious

Look for the first point where one of these changes from valid to invalid:
- pointer or reference identity
- length, index, or capacity
- enum or state-machine value
- return code or error branch
- ownership or lifetime assumptions
- parsed input or unit conversion

### 5. Report Evidence-Backed Suspect Locations

Finish with a short report in this shape:
1. symptom and debug mode used
2. suspect code locations
3. evidence for each location
4. confidence and open questions
5. next breakpoint or watchpoint if more work is needed

Only list a code location when at least one concrete observation supports it.

## Command And Reference Guide

Read these files only when needed:
- `references/gdb-command-recipes.md`: concrete command recipes for crash, hang, wrong-result, stepping, watchpoints, and optimized builds
- `references/gdb-risk-checklist.md`: confirmation template and risk checklist for attach, continue, function calls, memory writes, and signals

## Practical Heuristics

Apply these habits during debugging:
- keep a short scratch log of every confirmed observation
- prefer one new probe per iteration so you know what changed
- verify the thread count early and again after attach or restart
- distrust optimized-out locals and inlined frames; say when evidence is weak
- stop stepping once you have identified the first bad transition and shift to reporting

## Output Contract

Return a concise result with:
- `Suspect locations:` one or more concrete `path:line` or `function` entries
- `Why:` the observation that makes each location suspicious
- `Evidence:` the gdb commands or frames that produced the observation
- `Next:` the next highest-value probe if uncertainty remains

If the user's breakpoint hint was misleading, say so plainly and explain what the runtime evidence contradicted.

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-test-gdb-single-thread-debug version=0.1.0 author=Tony Zhang`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

