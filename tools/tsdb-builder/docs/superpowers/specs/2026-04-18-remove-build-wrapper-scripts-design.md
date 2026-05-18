# Remove deprecated build wrapper scripts

## Problem

`build-core.sh` and `build-others.sh` are deprecated wrapper entrypoints that duplicate behavior now available through `build.sh`. Keeping them around preserves two extra codepaths, stale image naming, and extra documentation surface.

The user wants both scripts deleted and wants confirmation that their functional intent is already covered by `build.sh`.

## Scope

In scope:

- delete `build-core.sh`
- delete `build-others.sh`
- update current user-facing documentation and guidance that still refers to these scripts
- add focused verification that `build.sh` covers the removed workflows

Out of scope:

- rewriting historical design/spec documents
- adding compatibility shims or stub wrappers
- changing the behavior of `build.sh` beyond what is required to prove coverage

## Coverage mapping

The removed scripts map to `build.sh` as follows:

| Removed script | Functional intent | `build.sh` replacement |
| --- | --- | --- |
| `build-core.sh` | clean full core build into `debug/` using the core image | `./build.sh --image core --clean core-all` |
| `build-others.sh` | clean full others build into `debug-others/`, excluding TAOSX and ODBC in the CI-style path | `./build.sh --image others --clean others-all -DBUILD_TAOSX=OFF -DBUILD_ODBC=OFF` |

This is sufficient because `build.sh` already owns:

- image selection
- output directory selection (`debug/` vs `debug-others/`)
- cache mounts
- Conan profile repair
- jobserver fallback
- taosx `dist/` cleanup logic
- Harbor image resolution

## Implementation approach

1. Remove the two wrapper scripts outright.
2. Update active docs (`README.md`, `.github/copilot-instructions.md`) so they no longer position those scripts as supported entrypoints.
3. Add or extend focused smoke verification that checks the exact `build.sh` invocations which replace the deleted scripts.
4. Re-run smoke, shell syntax, and diff checks.

## Error handling and compatibility

There is no compatibility layer. Deletion is intentional. The supported replacement is the explicit `build.sh` command line documented above.

Because the user asked to clean only active docs, historical design notes may continue to mention the removed scripts as part of past designs. That is acceptable for this change.

## Testing

Verification must show:

- the replacement `build.sh` commands are documented as the supported path
- focused smoke coverage exists for the replacement behavior
- repository shell checks and diff checks stay clean after removal
