# TDengine Stream Processing Architecture Skill

This skill explains the architecture of TDengine's stream processing system using analogies, ASCII diagrams, and code walkthroughs. Designed to onboard new developers quickly.

## What it does

When invoked with `/tsdb-dev-stream-arch`, Claude will explain stream concepts by:
1. Starting with an analogy to something familiar
2. Drawing ASCII architecture diagrams
3. Walking through the actual code path with file names and function signatures
4. Highlighting common gotchas and misconceptions

The skill embeds a comprehensive knowledge base covering:
- The Reader/Trigger/Runner task model
- Key data structures (`SStreamTask`, `SStreamReaderTask`, `SStreamRunnerTask`, etc.)
- Complete data path from `CREATE STREAM` to result output
- Source file map (new-stream library, mnode, vnode, snode)
- Trigger types, state machine, message types, constants
- Data sink, merger, checkpoint, and heartbeat subsystems

## Setup

```bash
cp -r tsdb-dev-stream-arch ~/.claude/skills/
```

Then type `/tsdb-dev-stream-arch` in Claude Code to activate.

## Example questions

- "How does stream work?"
- "Explain the trigger task"
- "How does the data sink write results?"
- "What happens when a node fails during stream processing?"
- "How does the merger combine data from multiple vnodes?"
