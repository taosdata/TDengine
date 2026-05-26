---
name: "tsdb-build-linux"
description: "Use when the user asks to run commands, inspect logs, build TDinternal, or execute pytest on the Linux repro host `192.168.127.101`, especially for tasks mentioning `/home/simon/dev/TDinternal`, remote VirtualTables verification, Linux ASAN/LSAN reruns, SSH remote commands, or TDinternal Linux-only test reproduction."
metadata:
  author: Jing Sima
  version: 1.0.0
  owner_team: engine
  compatibility: "Project-local TDinternal skill for local Codex usage under ~/.codex/skills; requires `sshpass` and runtime secret `TDINTERNAL_LINUX_101_PASSWORD`."
---

# TDinternal Linux 101

## When to use

- The user asks to run anything on `simon@192.168.127.101`.
- The task is about building, testing, or inspecting logs for TDinternal on the Linux repro machine.
- The request references `/home/simon/dev/TDinternal`, remote `community/test`, Linux-only repro, or remote ASAN/LSAN validation.

## Inputs

- Required:
  - the remote action to run,
  - or the TDinternal repo/test command to wrap.
- Usually assumed defaults:
  - host: `simon@192.168.127.101`
  - repo root: `/home/simon/dev/TDinternal`
  - build dir: `/home/simon/dev/TDinternal/debug`
  - test dir: `/home/simon/dev/TDinternal/community/test`
  - venv activation for pytest: `source ~/myenv/bin/activate`
- Missing-input clarification rule:
  - if the user asks for pytest/build/log work but does not specify the target file or command, ask for the minimal missing test path, log path, or command scope.

## Output

- Return the exact remote command or wrapper used.
- Report the key remote result, including pass/fail, build outcome, or the relevant log evidence.
- For leak-validation work, include whether raw ASAN/LSAN logs were inspected or whether that step is still pending.
- If the action was not executed, state the blocker precisely.

## Safety

- This skill operates on a real remote host. Treat arbitrary remote commands as at least medium risk.
- Do not hardcode or print the SSH password. The caller must provide `TDINTERNAL_LINUX_101_PASSWORD` in the environment.
- Prefer the narrowest wrapper script that matches the task instead of raw remote shell.
- Do not run multiple TDinternal pytest sessions in parallel on this host unless runtime isolation is explicit.
- For mutating or broad-scope commands outside the standard build/test wrappers, prefer `--dry-run` first and require explicit user intent before execution.
- For ASAN/LSAN acceptance, inspect raw runtime leak logs instead of relying only on wrapper summaries.

## Workflow

1. Prefer the bundled scripts instead of rewriting `sshpass` commands.
2. For arbitrary remote commands, use `scripts/remote_exec.sh`.
3. For TDinternal repo-root commands, use `scripts/tdinternal_exec.sh`.
4. For builds, use `scripts/tdinternal_build.sh`.
5. For pytest, use `scripts/tdinternal_pytest.sh ...`.
6. If the task is leak validation, follow the pytest run with direct raw log inspection on the remote host.

## Supporting Files

- Scripts:
  - [scripts/remote_exec.sh](scripts/remote_exec.sh)
  - [scripts/tdinternal_exec.sh](scripts/tdinternal_exec.sh)
  - [scripts/tdinternal_build.sh](scripts/tdinternal_build.sh)
  - [scripts/tdinternal_pytest.sh](scripts/tdinternal_pytest.sh)

## Common Commands

Build:
```bash
scripts/tdinternal_build.sh
```

Run one pytest file:
```bash
scripts/tdinternal_pytest.sh cases/05-VirtualTables/test_vtable_query_same_db_ntb_window_state.py
```

Run an arbitrary repo command:
```bash
scripts/tdinternal_exec.sh "git status --short"
```

Preview a mutating generic remote command without executing it:
```bash
scripts/remote_exec.sh --dry-run "rm -f /tmp/example"
```

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-build-linux version=1.0.0 author=Jing Sima`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
