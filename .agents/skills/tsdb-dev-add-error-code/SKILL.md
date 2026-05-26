---
name: tsdb-dev-add-error-code
description: Summarize the TDinternal workflow for adding or updating a TDengine error code. Use when asked to add a new `TSDB_CODE_*`, wire it into `taoserror.h` and `terror.c`, update the Chinese and English error-code docs, or verify error-code consistency.
metadata:
  author: Joey Sima
  version: 1.0.0
  owner_team: engine
  compatibility:
    - repo: /Users/simondominic/dev/TDinternal/community
    - files:
        - include/util/taoserror.h
        - source/util/src/terror.c
        - docs/zh/14-reference/09-error-code.md
        - docs/en/14-reference/09-error-code.md
---

# tsdb-dev-add-error-code

Use this skill when the task is to add a new TDengine error code in TDinternal, replace an overly generic error with a more specific one, or verify that an error-code change was propagated through code and docs.

## Inputs

- The failing scenario and the module or call path that owns it.
- Whether the error is user-visible or internal-only.
- The intended macro prefix if it is already known, for example `TSDB_CODE_MND_*` or `TSDB_CODE_VTABLE_*`.
- The target code path that should return the new code.

## Output

- A minimal patch that updates the canonical error-code definition, runtime string mapping, and both docs.
- A short verification note that lists the chosen code value, touched files, and checks that were run.

## Safety

- Some error-code families are module-owned and must not be reused outside that module, even if the text looks semantically close. Treat prefixes such as `TSDB_CODE_PAR_*` as parser-owned unless the codebase already shows an explicit cross-module convention.
- Do not add a new code if an existing specific code already matches the failure.
- Only consider reusing an existing code after confirming it belongs to the same owning module or to a truly shared/common family.
- Do not default to `*_INTERNAL_ERROR` when the failure can be expressed more precisely.
- Append within the correct active module block. Do not reuse commented-out legacy slots and do not reorder existing codes.
- Treat both docs as required. The repo has CI for header-vs-doc consistency.
- Do not skip `community/source/util/src/terror.c`. If the mapping is missing, `tstrerror()` can return an empty string.
- If the owning module is unclear, inspect nearby code first instead of guessing a prefix or hex range.

## Workflow

### 1. Confirm a new code is needed

Search for an existing code that already covers the failure:

```bash
rg -n "TSDB_CODE_.*<keyword>" community/include/util/taoserror.h
```

Prefer reusing an existing precise code over creating a duplicate semantic.

Before reusing any existing code, classify the code family:

- Shared families such as common/util can be candidates when they truly describe the failure.
- Module-owned families must stay inside their owning layer.
- Example: if the failure is not raised by parser logic, do not reuse `TSDB_CODE_PAR_*` just because the wording looks close.

### 2. Find the owning module block

Use the touched code path and sibling macro names to locate the correct block in `community/include/util/taoserror.h`.

- Follow the existing block comments and macro prefixes instead of guessing from memory.
- Match the new code family to the module that owns the failing logic.
- Keep the new code at the tail of the active block.
- Preserve commented-out legacy or unused values as-is.

### 3. Add the canonical macro in `taoserror.h`

Add a new macro in the chosen block:

```c
#define TSDB_CODE_<NAME> TAOS_DEF_ERROR_CODE(0, 0xNNNN)
```

Rules:

- Use a monotonic new value within that block.
- Keep the name aligned with the module prefix and existing naming style.
- If the code is user-visible, pick a business-specific name, not a generic fallback.

### 4. Add the runtime string in `terror.c`

Add the matching `TAOS_DEFINE_ERROR(...)` entry in the same logical block inside `community/source/util/src/terror.c`.

Example shape:

```c
TAOS_DEFINE_ERROR(TSDB_CODE_<NAME>, "Short user-facing message")
```

Rules:

- Keep the wording short and readable in logs.
- Keep the entry near sibling codes from the same module.
- Do not rely on other checks to catch this; the built-in doc consistency test does not validate `terror.c`.

### 5. Update both error-code docs

Add rows for the full hex code in:

- `community/docs/zh/14-reference/09-error-code.md`
- `community/docs/en/14-reference/09-error-code.md`

Rules:

- Put the row in the matching module section.
- Fill all columns, not just the description.
- Keep the Chinese and English rows semantically aligned.

### 6. Use the code at the call site

When wiring the new code into logic:

- Return the specific code from the failing function.
- Set `terrno = code` on failure paths when this layer returns an error.
- If the error path calls more functions before returning, preserve the chosen code so `terrno` is not overwritten.
- Add an error log before returning from a `code` + `_return` path when the failure is not a pure pass-through.

### 7. Verify

First, confirm the symbol reached all four canonical files:

```bash
rg -n "TSDB_CODE_<NAME>" \
  community/include/util/taoserror.h \
  community/source/util/src/terror.c \
  community/docs/zh/14-reference/09-error-code.md \
  community/docs/en/14-reference/09-error-code.md
```

Then run the repo's consistency check:

```bash
cd /Users/simondominic/dev/TDinternal/community/test
pytest --clean cases/81-Tools/01-Check/test_check_error_code.py
```

If the new code is actually used by runtime logic, also run the targeted build or regression tests for the touched module.

## Repo Notes

- The canonical macro list is `community/include/util/taoserror.h`.
- The runtime string map is `community/source/util/src/terror.c`.
- Prefix families are meaningful ownership boundaries, not just naming style. For example, `TSDB_CODE_PAR_*` is parser-scoped and should not be reused by non-parser modules.
- The existing CI case `community/test/cases/81-Tools/01-Check/test_check_error_code.py` checks header-to-doc consistency for the Chinese and English docs, but not `terror.c`.
- This means a safe workflow always includes a manual four-file grep plus the pytest case above.

## Final Response Checklist

- State the chosen module and numeric code.
- List the exact files changed.
- Say whether the docs consistency pytest was run.
- Say whether targeted runtime/build verification was run.

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-add-error-code version=1.0.0 author=Joey Sima`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
