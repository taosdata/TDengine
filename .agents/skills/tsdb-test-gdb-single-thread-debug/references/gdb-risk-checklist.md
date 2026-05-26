# GDB Risk Checklist

## Use This File When

Read this file before attaching to a live process, continuing a production workload, calling functions from gdb, modifying variables, writing memory, or sending signals.

## High-Risk Actions

| Action | Main Risk | Safer Default |
| --- | --- | --- |
| `attach PID` on a live service | stops the process and may trip timeouts | prefer core file or local reproduction |
| `continue` after attach | resumes a process in a changed timing environment | confirm impact window and rollback plan |
| `call foo()` | executes code with side effects | inspect state first; avoid unless essential |
| `set variable x=...` | mutates program behavior and can hide the bug | keep session read-only |
| `signal`, `kill`, `detach` | changes process lifecycle | confirm operator intent first |
| deleting or rewriting crash artifacts | destroys evidence | copy artifacts, never mutate originals |

## Required Confirmation Pattern

Before a high-risk action, say all three things:

1. the exact action you want to take
2. the likely impact
3. the dry-run or lower-risk alternative

Use a prompt like this:

```text
Risk note: attaching gdb to PID 1234 will pause the live process and may cause request timeouts.
Dry-run option: I can first show the exact gdb command file and inspection steps without executing them.
Please confirm if you want me to proceed with the live attach.
```

## Dry-Run Checklist

Before execution, verify:
- binary path
- arguments or PID
- working directory
- whether debug symbols are available
- whether the target is confirmed single-threaded
- whether the command file contains only read-only commands for the first pass

## Read-Only First-Pass Commands

Prefer this initial set:

```gdb
info threads
bt full
frame 0
info args
info locals
list
```

Avoid these until explicitly confirmed:

```gdb
call ...
set variable ...
set {int}addr = 1
signal SIG...
```
