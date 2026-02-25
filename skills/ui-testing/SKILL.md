---
name: ui-testing
description: |
  CRITICAL: Playwright UI testing for taosX Explorer Vue 3 application.
  Triggers on: playwright test, UI test, e2e test, explorer test, test explorer,
  test task creation, test login, UI automation, browser test, test taos explorer,
  测试UI, 端到端测试, playwright 测试, 浏览器测试, 界面测试
---

# UI Testing for taosX Explorer

> Playwright end-to-end testing skill for the Vue 3-based taosX Explorer (taos-explorer) web application.

## What this skill is for

- Writing **stable, executable** Playwright tests under `explorer/tests/`.
- Tests run against a **docker/integrated real environment** (真实后端环境) and should follow **real user workflows**.
- Key focus areas today: Login, Explorer(SQL), DataIn(TMQ), Management.

## Ground rules (CRITICAL)

### 1) Force English UI

All Explorer UI tests must force English by setting:

- `localStorage.local_language = 'en'`

This is already implemented in:

- `explorer/tests/_utils/test.ts` (fixture)
- `explorer/tests/global.setup.ts` (global auth generation)

Why: DataIn datasource configs differ between `zh` and `en` (including nested field UUIDs under `groups_after`). For stable selectors, tests are written against the **English** config.

### 2) Prefer the shared helpers

Do not duplicate fragile logic in every spec. Reuse helpers in `explorer/tests/_utils/`:

- `test.ts` — test wrapper that forces English via `page.addInitScript`
- `routes.ts` — route constants
- `auth.ts` — login helper
- `explorerSql.ts` — run SQL in `/explorer` via CodeMirror
- `datain.ts` — DataIn navigation/select helpers + task row operations

### 3) Real environment, serial execution

Explorer UI tests share a single environment. The Playwright config runs serially:

- `explorer/playwright.config.ts`: `workers: 1`, `fullyParallel: false`, `timeout: 60_000`

## Quick Start

### Install

```bash
pnpm -C explorer install
pnpm -C explorer exec playwright install --with-deps
```

### Run tests

```bash
# Run all tests
pnpm -C explorer exec playwright test

# Run a single spec
pnpm -C explorer exec playwright test tests/login.spec.ts
pnpm -C explorer exec playwright test tests/task-creation.spec.ts

# Headed / debug
pnpm -C explorer exec playwright test --headed
pnpm -C explorer exec playwright test --debug
```

### Useful env vars

```bash
# Override baseURL (defaults to http://localhost:6060)
PLAYWRIGHT_BASE_URL=http://localhost:6060 pnpm -C explorer exec playwright test

# Skip regenerating authenticated storageState when it already exists
PLAYWRIGHT_SKIP_GLOBAL_SETUP=true pnpm -C explorer exec playwright test
```

## Current test suite layout

```
explorer/tests/
├── _utils/
│   ├── auth.ts
│   ├── datain.ts
│   ├── explorerSql.ts
│   ├── routes.ts
│   └── test.ts
├── global.setup.ts
├── login.spec.ts
├── explorer.spec.ts
├── task-creation.spec.ts
├── tmq-task.spec.ts
└── management-menu.spec.ts
```

## Core patterns (recommended)

### Use the shared test fixture

All new specs should import `test` from `./_utils/test` (NOT from `playwright/test`) so English is forced consistently:

```ts
import { test, expect } from './_utils/test';
```

### Auth: storageState by default

- Auth is generated once in `explorer/tests/global.setup.ts` and stored as `explorer/tests/.auth/root.json`.
- By default, `explorer/playwright.config.ts` loads that state.

If you need an **unauthenticated** test, explicitly override:

```ts
test.use({ storageState: { cookies: [], origins: [] } });
```

### Prefer helper APIs

- Login (for special cases): `login(page, 'root', 'taosdata')` from `explorer/tests/_utils/auth.ts`
- SQL:
  - `runSql(page, 'select server_version();')`
  - `runSqlBatch(page, ['CREATE DATABASE ...', 'CREATE TOPIC ...'])`
- DataIn:
  - `gotoDataInTask(page)`
  - `openAddSourceFromList(page)`
  - `selectElOptionByText(page, 'targetDB', dbName)`
  - `findTaskRow(page, taskName)`
  - `startTaskFromRow(page, row)`

### Element Plus tricky parts (do this, or you will get flaky tests)

#### `el-select`

Element Plus may attach the `id` on an internal readonly `<input>`; placeholder overlay can intercept clicks.

Use the helper `selectElOptionByText` (force click + scope visible dropdown):

- click root with `{ force: true }`
- select option from `.el-select-dropdown:visible`

#### Hover-driven operations menus

Some DataIn task row actions are only visible after hover.

Use `openRowOperations(page, row)` (hover row -> hover button -> wait for `.el-dropdown-menu:visible`).

## Writing new tests (guidelines)

- Use unique resource names: `const ts = Date.now();` then `e2e_xxx_${ts}`.
- Avoid `page.waitForTimeout()`; prefer `expect(locator).toBeVisible()/toBeEnabled()`.
- Keep tests black-box and user-facing: click what users click; assert what users see.
- If selectors are unstable, prefer pushing UI changes (e.g., `data-testid`) over writing brittle locators.

## Debugging

```bash
pnpm -C explorer exec playwright test --debug
pnpm -C explorer exec playwright test --headed
pnpm -C explorer exec playwright show-report
```

## References
- Playwright config: `explorer/playwright.config.ts`
- Global auth + English: `explorer/tests/global.setup.ts`
- Helpers: `explorer/tests/_utils/`
- Test Spec (TS): `docs/specs/2026-02-25-explorer-ui-overall-TS.md`
- Test Plan: `docs/dev/UI_TEST_CASES_PLAN.md`

## Instructions for agents
When the user asks about Explorer UI testing:
1. Confirm the integrated environment is running and `PLAYWRIGHT_BASE_URL` is correct.
2. Always use the shared fixture `explorer/tests/_utils/test.ts` (forces English).
3. Prefer helpers (`auth.ts`, `explorerSql.ts`, `datain.ts`, `routes.ts`) over re-implementing flows.
4. Assume authenticated storageState by default; for unauth flows override `test.use({ storageState: { cookies: [], origins: [] } })`.
5. For Element Plus `el-select` and hover menus, use the existing helper functions to avoid flakiness.
6. Avoid `waitForTimeout`; wait on locators / URL / enabled state.
7. For new DataIn datasource coverage, follow the per-datasource tracking items in `docs/dev/UI_TEST_CASES_PLAN.md`.
