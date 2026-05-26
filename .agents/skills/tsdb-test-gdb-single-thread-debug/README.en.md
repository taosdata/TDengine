# GDB Single-Thread Debug Skill

## Overview

`tsdb-test-gdb-single-thread-debug` is an Agent Skill for investigating failures in single-threaded native programs, including crashes, hangs, wrong results, assertion failures, and corrupted state.

It helps an agent start from the user's symptom, optional breakpoint hints, a core file, or a local reproduction path, then narrow the issue to evidence-backed suspect code locations with GDB.

## What It Can Do

- Debug from a binary, local reproduction flow, or core file
- Turn user breakpoint hints into a reusable GDB execution plan
- Prefer read-only inspection to reduce risk to the target process or artifacts
- Report suspect locations, evidence, confidence, and next-step probes

## Layout

```text
tsdb-test-gdb-single-thread-debug/
|-- SKILL.md
|-- README.zh-CN.md
|-- README.en.md
|-- agents/
|   `-- openai.yaml
|-- references/
|   |-- gdb-command-recipes.md
|   `-- gdb-risk-checklist.md
`-- scripts/
    `-- render_gdb_plan.py
```

## Key Files

- `SKILL.md`: the main skill definition, including scope, inputs/outputs, safety rules, and debugging workflow
- `scripts/render_gdb_plan.py`: generates a dry-run plan and `.gdb` command file from a binary, core file, and breakpoint hints
- `references/gdb-command-recipes.md`: command recipes for common debugging scenarios
- `references/gdb-risk-checklist.md`: confirmation checklist for high-risk actions
- `agents/openai.yaml`: agent-related configuration

## Good Fit Scenarios

Recommended when:

- A single-threaded program crashed and produced a core file
- A crash, hang, or wrong-result issue can be reproduced locally
- You want to quickly turn breakpoint hints into an executable GDB plan
- You want a low-risk, read-only first-pass investigation

Not a direct fit for:

- Complex multi-threaded debugging problems
- Cases that require online memory mutation or heavy side-effectful expressions
- Situations where no binary, core file, or reproduction path is available

## Typical Workflow

1. Collect the target binary, arguments, core file, symptom summary, and optional breakpoint hints
2. Use `render_gdb_plan.py` to generate a dry-run plan and `.gdb` command file
3. Start with read-only inspection such as stack frames, locals, arguments, and key memory
4. Narrow the issue to the first transition from valid state to invalid state
5. Output evidence-backed suspect locations and the next best probe

## Next Steps

The next phase is to connect the current GDB debugging capability with build, test, log analysis, core collection, and fix-suggestion workflows so it can become a more complete automated troubleshooting loop.

The target direction includes:

- Integrating with build and symbol-generation flows to reduce debug setup cost
- Connecting with test and reproduction workflows so investigation can start automatically after reproduction
- Combining core analysis, log analysis, and code-fix suggestions into a more reliable repair pipeline
- Gradually evolving toward a robot that can automatically fix core-related issues

## Development Information
- Author: Tony Zhang
- Owner Team: Query Group, Engine Group
- Version: 0.1.0