# TDinternal Windows Build Skill

This skill encapsulates a verified Windows build workflow for the TDinternal repository.

## What it does

- Uses Visual Studio 2022 developer environment (`vcvars64.bat`)
- Configures CMake in `D:/TDinternal/debug`
- Uses `NMake Makefiles JOM` generator
- Builds with `jom`
- Provides one single compile process entry

## Verified workflow

1. Enter `D:/TDinternal/debug`
2. Run `vcvars64.bat`
3. Configure: `cmake .. -G "NMake Makefiles JOM"`
4. Build: `jom`

## Files

- `skill.md`: skill definition and usage guidance
- `config.json`: metadata and triggers
- `implementation.ps1`: executable helper functions

## Notes

- This skill assumes Visual Studio 2022 Community is installed in the default location.
- It intentionally uses CMD invocation for environment setup compatibility.
