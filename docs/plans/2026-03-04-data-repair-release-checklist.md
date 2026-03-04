# TDengine Data Repair Release Checklist (2026-03-04)

## 1. Scope

- `P7`: copy mode hardening (`T7.4`, `T7.5`)
- `P8`: fixture generation, mode matrix, docs, release gate (`T8.1`~`T8.4`)

## 2. Verification Commands

All commands were executed in `/Projects/work/TDengine` on `2026-03-04`.

```bash
bash tests/ci/repair_mode_matrix.sh
ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure
cmake --build debug --target taosd
```

## 3. Verification Results

1. `repair_mode_matrix.sh`: passed
   - `force(tsdb)` passed
   - `force(meta)` passed
   - `replica` passed
   - `copy` passed
2. `ctest -R commonTest`: passed (`100% tests passed, 0 failed`)
3. `cmake --build debug --target taosd`: passed

## 4. Release Risks

1. Environment dependency risk:
   - External dependency fetch (`ext_pcre2`) may intermittently fail in restricted network environments.
   - Mitigation: retry `cmake --build` with approved elevated permissions when needed.
2. Runtime dependency risk for `copy` mode:
   - Requires reachable `ssh/scp` and valid `--replica-node=<host>:<absolute-path>`.
   - Mitigation: pre-check endpoint format; inspect `repair.log` for `copy dispatch detail` and `consistency=verified`.
3. Operational misuse risk:
   - Wrong `vnode-id` or wrong mode selection may target unintended repair path.
   - Mitigation: explicit parameter validation and mandatory backup path recommendation.

## 5. Sign-off

- Release gate result: `PASS`
- Recommendation: proceed to packaging/review and merge workflow.
