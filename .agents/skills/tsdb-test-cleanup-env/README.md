# Cleanup Test Environment Skill

This skill encapsulates a verified cleanup workflow for the TDengine test environment on Linux and Windows.

## What it does

- Detects operating system and uses the matching command set
- Kills all running `taosd` processes
- Clears all contents inside the `sim` directory without deleting the directory itself
- Deletes the `test/run` directory completely
- Provides one single cleanup process entry

## Verified workflow

1. Detect current OS
2. Kill all `taosd` processes
3. Clean `sim` directory contents
4. Delete `test/run` directory
5. Confirm cleanup results

## Files

- `skill.md`: skill definition and usage guidance
- `config.json`: metadata and triggers

## Notes

- Linux paths:
  - `sim`: `/home/zbm/td/sim`
  - `test/run`: `/home/zbm/td/community/test/run`
- Windows paths:
  - `sim`: `D:\td\sim`
  - `test/run`: `D:\td\community\test\run`
