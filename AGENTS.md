# Repository Guidelines

## Project Structure & Module Organization
- `source/` contains core engine code, organized by subsystem (`dnode/`, `client/`, `libs/`, `os/`, `util/`).
- `include/` stores exported headers, largely mirrored by subsystem.
- `tools/`, `utils/`, and `examples/` provide operational binaries, helper utilities, and sample client usage.
- `contrib/` holds bundled third-party dependencies; `cmake/` contains build options and platform logic.
- `test/` is the newer pytest-based end-to-end framework (`cases/`, `env/`, `new_test_framework/`).
- `tests/` contains unit/system/legacy/perf/chaos suites and CI runner scripts.

## Build, Test, and Development Commands
- `./build.sh gen`: configure a debug build in `debug/` with tests/tools enabled.
- `./build.sh bld`: compile using available CPU cores.
- `./build.sh test`: run CTest from the configured build directory.
- `cmake -B debug -DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true && cmake --build debug`: direct CMake equivalent.
- `cd test && pytest cases/...`: run new pytest cases.
- `cd tests && ./run_all_ci_cases.sh -t python`: run Python-based system CI cases.
- `./build.sh conan-build-all`: optional Conan-driven dependency + build pipeline.

## Coding Style & Naming Conventions
- C/C++ formatting follows `.clang-format`: 2-space indentation, 120-column limit, no tabs, sorted includes.
- Python formatting uses `black` (see `.pre-commit-config.yaml`).
- Run `pre-commit run -a` before pushing; hooks validate YAML/JSON, whitespace, typos, and run `cppcheck`.
- Pytest files should follow `test_*.py` naming (for example, `test_insert_double.py`).

## Testing Guidelines
- Configure with `-DBUILD_TEST=true` so CTest targets are generated.
- Add C/C++ unit tests near the target module and register with CMake `add_test`.
- Add end-to-end coverage under `test/cases/`; reuse deployment YAMLs from `test/env/`.
- For every change, include at least one automated verification path and list exact test commands in the PR description.

## Commit & Pull Request Guidelines
- Branch from `3.0` (not `main`); docs-only branches should start with `docs/`.
- Follow commit prefixes used in history: `fix:`, `feat:`, `docs:`, `chore:`, `test(scope):`.
- PRs should include problem summary, linked issue, design notes, and verification command output.
- Complete the contributor CLA before first code contribution.
