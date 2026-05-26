---
name: tsdb-build-windows
description: "Build TDinternal on Windows using VS2022 and JOM. Use when building TDinternal on Windows, setting up the CMake/NMake build environment, or troubleshooting Windows build failures. Keywords: Windows build, VS2022, JOM, NMake, CMake, TDinternal debug."
metadata:
  author: Simon Guan
  version: 1.0.0
  owner_team: engine
---

# TDinternal Windows Build Skill

## Purpose

Provide a repeatable, verified build flow for TDinternal on Windows.

## Scope

- Configure/build in `D:/TDinternal/debug`
- Use VS2022 environment via `vcvars64.bat`
- Use CMake generator: `NMake Makefiles JOM`
- Build with `jom`
- Keep only one compile workflow entry

## Primary workflow

1. Ensure `D:/TDinternal/debug` exists.
2. Open command environment with VS2022 `vcvars64.bat`.
3. Configure CMake:
   - `cmake .. -G "NMake Makefiles JOM"`
4. Build:
   - `jom`

## Single workflow function

- `Invoke-TDinternalBuild`
  - Configure: `cmake .. -G "NMake Makefiles JOM"`
  - Build: `jom`

## Troubleshooting

- If generator mismatch appears: use only one build directory per generator.
- If environment variables are missing: run through `cmd /c "...vcvars64.bat && ..."`.

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-build-windows version=1.0.0 author=Simon Guan`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
