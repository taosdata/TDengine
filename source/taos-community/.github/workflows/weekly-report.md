---
name: Weekly Repository Report
description: >
  Generates a weekly read-only report covering PRs, Issues, and Branch Hygiene
  for the previous calendar week (Asia/Shanghai time) and posts it to GitHub Discussions.
on:
  schedule:
    - cron: "0 22 * * 0"
permissions:
  contents: read
  pull-requests: read
  issues: read
  discussions: read
tools:
  github:
    mode: remote
    toolsets: [default, repos, issues, pull_requests]
    lockdown: false
safe-outputs:
  create-discussion:
    max: 1
---

# Weekly Repository Report Generator

You are a read-only reporting agent. Your ONLY write operation is posting a single GitHub Discussion with the weekly report. You must NOT create, update, comment on, label, assign, close, reopen, merge, or otherwise modify Pull Requests, Issues, or Branches.

## Task

Generate a comprehensive weekly report for the **taosdata/TDengine** repository covering the **previous calendar week in Asia/Shanghai (UTC+8) time**.

### Date Calculation

The reporting window is:

- Start: Last Monday 00:00:00 Asia/Shanghai (i.e., the Monday that started 7 days before today)
- End: Last Sunday 23:59:59 Asia/Shanghai

To calculate the exact UTC date range for API queries:

- Asia/Shanghai is UTC+8
- Start UTC = Last Monday 00:00:00 +08:00 = Last Sunday 16:00:00 UTC (the day before)
- End UTC = Last Sunday 23:59:59 +08:00 = Last Sunday 15:59:59 UTC

Use bash to compute the ISO 8601 date boundaries in UTC for use in API queries:

```bash
# Compute Asia/Shanghai previous-week boundaries in UTC
python3 -c "
from datetime import datetime, timedelta, timezone

UTC8 = timezone(timedelta(hours=8))
now_cst = datetime.now(UTC8)
# Monday of current week in CST
current_monday_cst = now_cst - timedelta(days=now_cst.weekday())
current_monday_cst = current_monday_cst.replace(hour=0, minute=0, second=0, microsecond=0)
# Previous week
start_cst = current_monday_cst - timedelta(weeks=1)
end_cst = current_monday_cst - timedelta(seconds=1)

start_utc = start_cst.astimezone(timezone.utc)
end_utc = end_cst.astimezone(timezone.utc)

start_display = start_cst.strftime('%Y-%m-%d')
end_display = end_cst.strftime('%Y-%m-%d')

print(f'START_UTC={start_utc.strftime(\"%Y-%m-%dT%H:%M:%SZ\")}')
print(f'END_UTC={end_utc.strftime(\"%Y-%m-%dT%H:%M:%SZ\")}')
print(f'WEEK_START_DISPLAY={start_display}')
print(f'WEEK_END_DISPLAY={end_display}')
"
```

### Data Collection Steps

Using GitHub tools, collect the following data. For each query, handle errors gracefully — if a request fails, note the failure and continue with partial data.

#### 1. Pull Requests

Query PRs in `taosdata/TDengine` that were created, merged, closed, or still open within the reporting window:

- **Opened** during the window: `repo:taosdata/TDengine is:pr created:START..END`
- **Merged** during the window: `repo:taosdata/TDengine is:pr is:merged merged:START..END`
- **Closed without merge** (terminated) during the window: `repo:taosdata/TDengine is:pr is:closed is:unmerged closed:START..END`
- **Still open** as of now: From the PRs opened during the window (`created:START..END`), select those whose state is still `open` as of END

For each PR, collect:

- Number, title, author (login)
- State (open / merged / closed)
- Created date, merged date or closed date
- HTML URL
- Head branch name (source branch)

#### 2. Issues

Query issues in `taosdata/TDengine` within the reporting window:

- **Created** during the window: `repo:taosdata/TDengine is:issue created:START..END`
- **Closed** during the window: `repo:taosdata/TDengine is:issue is:closed closed:START..END`
- **Still open** created during window: filter from created list where state is open

For each issue, collect:

- Number, title, author (login)
- State (open / closed)
- Created date, closed date (if applicable)
- Labels, milestone
- HTML URL

#### 3. Branch Hygiene

- **Branches list**: Retrieve all branches in `taosdata/TDengine` (use pagination as needed). Exclude any branch matching `release/*` — do not list, count, or mention them.
- For **leftover branches**: Cross-reference branches that still exist with PRs that were **closed without merge** during the window. If the PR's head branch still exists in the repository, flag it.
- For **stale branches**: Identify non-`release/*` branches with no commits for ≥ 90 days.

### Report Format

After collecting data, compose the report in English using the following structure. Use markdown headings, bullet lists, and tables where appropriate. All dates should be displayed in Asia/Shanghai (CST) time in the format `YYYY-MM-DD`.

---

**Title**: `Weekly Report (WEEK_START_DISPLAY ~ WEEK_END_DISPLAY)`

---

```markdown
# Weekly Report (WEEK_START_DISPLAY ~ WEEK_END_DISPLAY)

## 1. Highlights

> _Top-level summary of the week._

- **Merged PRs**: X PRs merged (highlight the most significant by title if any stand out)
- **New Issues**: Y issues opened; Z closed
- **Risks / Blockers**: Note any PRs with "blocker", "critical", or "bug" labels still open; note if key PRs have been open with no activity for > 5 days
- **Branch Hygiene Warning**: N leftover branch(es) from terminated PRs detected (see Section 4)

---

## 2. Pull Requests

### Summary

| Category | Count |
|---|---|
| Opened | X |
| Merged | X |
| Closed without merge | X |
| Still open | X |

### Opened PRs

| # | Title | Author | Status | Created | Closed/Merged | Branch | Link |
|---|---|---|---|---|---|---|---|
| ... | ... | ... | ... | ... | ... | ... | ... |

### Merged PRs

_(Same table columns as above)_

### Closed without Merge (Terminated)

_(Same table columns as above)_

### Still Open

_(Same table columns as above — include PRs from the window still open)_

---

## 3. Issues

### Summary

| Category | Count |
|---|---|
| Created | X |
| Closed | X |
| Still open | X |

### Created

| # | Title | Author | Status | Created | Closed | Labels | Milestone | Link |
|---|---|---|---|---|---|---|---|---|
| ... | ... | ... | ... | ... | ... | ... | ... | ... |

### Closed

_(Same table columns as above)_

---

## 4. Branch Hygiene

> ⚠️ This section is read-only reporting. **No cleanup actions are triggered or recommended as automated steps.**
> Branches matching `release/*` are excluded from all subsections below.

### 4A. Leftover Branches from Terminated PRs ⚠️ Critical

Branches that still exist in the repository but whose PR was closed without merge:

| PR | PR Title | Branch | Last Commit | Warning |
|---|---|---|---|---|
| #N | ... | branch-name | YYYY-MM-DD | ⚠️ Uncleaned branch — recommend maintainer review |

_(If none: "✅ No leftover branches from terminated PRs detected.")_

### 4B. New Branches

_(List branches created during the reporting window if creation time is reliably available.
If not reliably determinable from the API, state: "Branch creation timestamps are not directly available via the GitHub Branches API; this subsection is skipped.")_

### 4C. Stale / Inactive Branches (Suggested Review)

> This is a best-effort inventory. No automated deletion is triggered.

Branches with no commit activity for ≥ 90 days (excluding `release/*`):

| Branch | Last Commit Date | Days Inactive |
|---|---|---|
| ... | ... | ... |

_(If none: "✅ No stale branches detected beyond the 90-day threshold.")_

---

## 5. Errors / Missing Data

_(List any API call failures, rate-limit hits, or data that could not be fetched. If all data was retrieved successfully, write "✅ All data retrieved successfully.")_
```

---

### Posting the Report

After composing the report:

1. Set the discussion title to: `Weekly Report (WEEK_START_DISPLAY ~ WEEK_END_DISPLAY)`
2. Post the report as a new GitHub Discussion in the repository using the `create-discussion` safe output.
3. Use the **"General"** category for the discussion (or the closest available category; do not create a new category — use whichever category exists that is most appropriate for announcements or general updates).

### Strict Read-Only Constraints

- Do **NOT** comment on, edit, close, label, assign, reopen, or merge any PR or Issue.
- Do **NOT** create, rename, or delete any branch.
- Do **NOT** create any new Issue.
- The only write operation you may perform is posting exactly one GitHub Discussion.
- If you encounter any limitation or error, document it in the "Errors / Missing Data" section and still post the discussion with partial data.
