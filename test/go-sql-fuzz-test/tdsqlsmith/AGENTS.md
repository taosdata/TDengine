# Repository Guidelines

## Project Structure & Module Organization
- `cmd/tdsqlsmith/`: CLI entrypoint (`run`, `replay`, `serve`).
- `internal/`: core logic (run loop, report model, crash guard, executor, parser gate, API serve layer).
- `internal/corpusdata/`: embedded SQL corpus and grammar assets used at runtime (no external `sqlparse` checkout required).
- `web/console/`: Vue 3 + TypeScript frontend; build output goes to `internal/serve/webdist/` for embed.
- `bin/`: local compiled binary (`tdsqlsmith`).
- `out/`: generated runtime artifacts (`<run_id>/run_report.json`, `crash_guard/`).
- `run_parent_child_test.sh` and `run_web_service.sh`: operational scripts for long-run fuzzing and web service lifecycle.

## Build, Test, and Development Commands
- `make init`: install/update Go modules and frontend npm dependencies.
- `make build`: build frontend first, then build backend binary to `bin/tdsqlsmith`.
- `make package`: run build, collect binary + scripts, and generate a tarball in `bin/`.
- `go build -o ./bin/tdsqlsmith ./cmd/tdsqlsmith`: build backend binary directly.
- `go test ./...`: run Go unit/integration tests.
- `go test -tags=integration ./...`: run integration-tagged tests.
- `cd web/console && npm run build`: build frontend into `internal/serve/webdist`.
- `./run_web_service.sh start --daemon`: start web service with local binary.
- `DSN='root:taosdata@tcp(127.0.0.1:16030)/' ./run_parent_child_test.sh 10m`: start parent/child fuzz run.

## Coding Style & Naming Conventions
- Go formatting is mandatory: `gofmt -w` on edited files.
- Go conventions: lowercase package names, exported symbols in `CamelCase`, tests in `*_test.go`.
- Prefer deterministic behavior (seed-aware code paths, explicit config values).
- Frontend uses Vue SFC (`<script setup lang="ts">`) and should keep typed state and small composable helpers.

## Testing Guidelines
- Use Go `testing` with table-driven tests where practical.
- Add tests for parsing/report normalization and crash handling changes.
- For frontend changes, at minimum ensure `npm run build` succeeds.
- For runtime workflow changes, include a reproducible smoke command and expected artifact path.

## Commit & Pull Request Guidelines
- Follow concise, scoped commit subjects seen in history (for example: `feat: ...`, `docs: ...`, `run: ...`, `refactor: ...`).
- Keep commits focused; avoid mixing unrelated refactors and behavior changes.
- PRs should include: motivation, key changes, exact validation commands, and generated artifact paths.
- Include screenshots for UI changes and mention DSN/environment assumptions.

## Security & Configuration Tips
- Never commit credentials, tokens, or machine-local dumps.
- Keep DSN/ports configurable via flags or env; do not hardcode environment-specific endpoints.
- Do not commit generated frontend assets under `internal/serve/webdist` (only keep placeholder/document files tracked).
