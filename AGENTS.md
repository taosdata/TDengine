# TDengine Session Conventions

## Progress Reporting (Persistent Rule)
- Every progress report must include a visual progress bar.
- Use the task table in `task_plan.md` as the source of truth.
- Show at least:
  - overall percentage
  - completed/total tasks
  - bar visualization

## Required Progress Bar Format
- Use this format in each report:
  - `进度: <percent>% [<bar>] <done>/<total>`
- Bar width: 20 characters.
- Filled: `#`
- Empty: `-`

## Calculation Rule
- `done`: number of tasks with status `completed`.
- `total`: number of tasks with status in `{completed, in_progress, pending}`.
- `percent = done / total * 100` (keep one decimal place).
