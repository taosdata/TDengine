# Repository Guidelines

## Project Structure & Module Organization
- Core parser sources live at repository root.
- Grammar source: `td_sql.y` (edited by contributors).
- Generated parser: `sql.go` and debug table `y.output` (generated via `goyacc`).
- Lexer and keywords: `lexer.go`, `keyword.go`.
- AST and statement nodes: `ast.go`, `expr_*.go`, `stmt_*.go` (including temporary `*_stub.go` files used during migration).
- Lemon reference grammar (do not modify): `lemon/`.
- Migration/parity tooling: `tool/migrate/`.
- Test files: `*_test.go` in root; parity outputs under `reports/`.
- SQL corpora used by gates: `testdata/sql_corpus/`.

## Build, Test, and Development Commands
- `goyacc -o sql.go -v y.output td_sql.y`  
  Regenerates parser after grammar changes.
- `GOCACHE=/tmp/gocache go test ./... -count=1`  
  Runs all tests without cache.
- `go test ./... -run TestName -count=1`  
  Runs a focused test during iterative grammar work.
- `make test`  
  Project test shortcut.
- `make validate-parity` / `make baseline-parity`  
  Runs Lemon vs Go parity checks and baseline report generation.
- `make parser-hard-gate`  
  Full required gate: regenerate parser, run all tests, parity validation, query coverage, write/insert corpus gates, statement diff and roundtrip gates.
- `make statement-branch-gate`  
  Branch matrix gate for statement/query/select (including nested select matrix).
- `make query-coverage`  
  Query-related weighted coverage gate (target 100%).
- `make write-sql-gate` / `make insert-sql-gate`  
  Validates generated write/insert SQL corpora.

## Generated Artifacts Policy
- `reports/` and local `build/` are generated/intermediate artifacts and should not be committed.
- Regenerate reports on demand with:
  - `make validate-parity` (writes `reports/final/*`)
  - `make statement-diff` (writes statement rule matrix/diff/fix queue under `reports/final/`)
  - `make query-diff` (writes query rule matrix/diff/fix queue under `reports/query/`)
  - `make query-coverage` (writes `reports/query/coverage_summary.md`)
  - `make parser-hard-gate` (runs full gate and regenerates all required report outputs)
- After local analysis, clean intermediates with:
  - `rm -rf reports build`

## Coding Style & Naming Conventions
- Language: Go (`module sqlparser`, Go 1.25).
- Always format with `gofmt` before submitting.
- Keep grammar rules and semantic actions explicit and small; add one rule branch at a time.
- File naming:
  - tests: `*_test.go`
  - statement/AST nodes: `stmt_*.go`, `expr_*.go`
  - temporary migration nodes: `*_stub.go`

## Testing Guidelines
- Add tests for every grammar change before moving to next rule.
- For grammar edits, required loop:
  1. update `td_sql.y`
  2. regenerate with `goyacc`
  3. add/adjust targeted unit tests
  4. run full `go test ./...`
- Prefer branch-covering tests for new alternatives (empty branch + each non-empty branch).
- For parity work, run `make parser-hard-gate` before considering a task complete.
- `reports/final/*.md` and `reports/query/*.md` are generated artifacts and should be refreshed by the gate when grammar/statement behavior changes.

## Commit & Pull Request Guidelines
- Follow Conventional Commit style used in history, e.g.:
  - `feat(sqlparser): ...`
  - `refactor(sqlparser): ...`
- PRs should include:
  - summary of grammar branches migrated
  - updated/added tests
  - `goyacc` conflict count before/after
  - parity/coverage impact when relevant
- Never modify files under `lemon/`; it is the migration baseline.
- Do not commit local build artifacts under `build/`.
