# GDB Command Recipes

## Table Of Contents
- Entry modes
- First-pass inspection
- Breakpoints and watchpoints
- Crash recipe
- Hang recipe
- Wrong-result recipe
- Optimized-build notes
- Session logging

## Entry Modes

Use one of these entry patterns:

```bash
gdb -q --args BIN ARG1 ARG2
```

```bash
gdb -q BIN CORE_FILE
```

```bash
gdb -q BIN PID
```

Inside gdb, prefer these setup commands early:

```gdb
set pagination off
set breakpoint pending on
set print pretty on
set print elements 0
```

Check thread count immediately:

```gdb
info threads
```

If you see more than one thread, stop using this skill as the main procedure.

## First-Pass Inspection

Run these commands before you start stepping heavily:

```gdb
bt
bt full
frame 0
info args
info locals
list
```

Useful memory and expression inspection:

```gdb
print expr
print *ptr
x/16gx addr
x/s str_ptr
ptype var
```

## Breakpoints And Watchpoints

Typical breakpoints:

```gdb
break main
break file.c:128
break parser_init if cfg == 0
```

Temporary and conditional probes:

```gdb
tbreak file.c:128
condition 2 len < 0
ignore 3 99
```

Watchpoints are powerful but expensive. Use them on a small number of targets:

```gdb
watch state
watch *ptr
rwatch field
awatch shared_flag
```

List and manage probes:

```gdb
info breakpoints
disable 4
enable 4
delete 4
```

## Crash Recipe

For a crash or assertion failure:

```gdb
bt full
frame 0
info args
info locals
up
down
list
```

Then ask:
- which value is invalid in the crashing frame
- who last owned or wrote that value
- whether the caller violated a precondition

If the crashing frame is only the victim, move upward until you find the first frame that supplied the bad input.

## Hang Recipe

For a hang, dead loop, or no-response bug:

```gdb
interrupt
bt
frame 0
info locals
list
```

If stuck in a loop, inspect the loop condition and variables that should make progress:

```gdb
print i
print count
print state
```

Then place a narrow conditional breakpoint at the loop head or state transition:

```gdb
break worker.c:212 if i > count
```

## Wrong-Result Recipe

For corrupted output or bad state without a crash:

1. choose the earliest observable bad value
2. stop where that value is produced
3. compare expected and actual inputs at that point
4. walk backward to the first invalid transition

Useful commands:

```gdb
break file.c:produce_value
run
print expected
print actual
next
step
finish
```

If the value is overwritten later, add a watchpoint after you narrow the suspect storage.

## Optimized-Build Notes

Expect weaker evidence when:
- locals show as `<optimized out>`
- frames are inlined
- source lines appear to jump unexpectedly

Prefer these mitigations:
- rebuild with `-g`
- reduce optimization for the failing module if possible
- trust memory, arguments, and backtraces more than missing locals
- use `disassemble /m` only when source-level stepping is misleading

## Session Logging

Capture a reproducible record when the session becomes non-trivial:

```gdb
set logging file /tmp/gdb-session.log
set logging enabled on
```

Turn logging off when done:

```gdb
set logging enabled off
```
