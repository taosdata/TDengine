# Docs CD Branch-Based Success Notification Design

## Goal

Replace noisy URL/count-based docs CD success notifications with a compact message that reflects which site entrypoints should be reviewed, based on changed source branches rather than rendered HTML diffs.

## Decision Summary

- Keep success notifications, but stop printing URL lists, counts, and affected buckets
- Infer notification content from changed source branches participating in the deploy
- Only distinguish two classes per language:
  - `latest`
  - `next`
- Map `3.0` source-branch changes to `next`
- Map every other assembled source branch change to `latest`
- If both classes change in one deploy, print both links
- Keep baseline and failure notifications unchanged
- Keep no-change runs silent

## Problem

The previous HTML-diff-based success notification became noisy because English builds can drift at the asset-hash level even when source content is effectively unchanged. That made URL/count-based notifications hard to trust and hard to scan.

The operator need is not page-level diff output. The operator only needs to know which site entrypoints may have changed and where to click.

## Scope

This design changes only the success notification semantics for docs CD.

It does not change:

- deploy behavior
- branch assembly behavior
- baseline recording behavior
- failure notification behavior
- no-change silent behavior

## Source-Branch Classification

For each language, classify changed assembled source branches into two output buckets:

- `next`
  - source branch: `3.0`
- `latest`
  - all other assembled source branches for that language, including `main`, `3.3.6`, `docs-cloud`, and any other non-`3.0` assembled branch

This intentionally collapses all non-`3.0` branches into one user-facing message because the operator does not want per-version detail.

## Site Mapping

### Chinese site

- `latest` → `https://docs.taosdata.com`
- `next` → `https://docs.taosdata.com/next`

### English site

- `latest` → `https://docs.tdengine.com`
- `next` → `https://docs.tdengine.com/next`

## Success Notification Format

### English latest only

```text
✅ docs-cd deployed
job: https://git.tdengine.net/rd-public/tsdb/-/jobs/35788
Click to visit:
latest: https://docs.tdengine.com
```

### English next only

```text
✅ docs-cd deployed
job: https://git.tdengine.net/rd-public/tsdb/-/jobs/35788
Click to visit:
next: https://docs.tdengine.com/next
```

### English latest + next

```text
✅ docs-cd deployed
job: https://git.tdengine.net/rd-public/tsdb/-/jobs/35788
Click to visit:
latest: https://docs.tdengine.com
next: https://docs.tdengine.com/next
```

### Chinese latest only

```text
✅ docs-cd deployed
job: https://git.tdengine.net/rd-public/tsdb/-/jobs/35787
点击查看:
latest: https://docs.taosdata.com
```

### Chinese next only

```text
✅ docs-cd deployed
job: https://git.tdengine.net/rd-public/tsdb/-/jobs/35787
点击查看:
next: https://docs.taosdata.com/next
```

### Chinese latest + next

```text
✅ docs-cd deployed
job: https://git.tdengine.net/rd-public/tsdb/-/jobs/35787
点击查看:
latest: https://docs.taosdata.com
next: https://docs.taosdata.com/next
```

## Behavioral Rules

- If no relevant source branch changed for a language, do not send a success notification for that language
- If only `3.0` changed, print only `next`
- If only non-`3.0` assembled branches changed, print only `latest`
- If both `3.0` and non-`3.0` assembled branches changed, print both `latest` and `next`
- The link label should appear once per notification, not once per URL

## Data Source

The notification should use source-branch change detection, not rendered HTML diff output, as the basis for deciding which links to print.

That means the implementation should persist and compare per-language source-branch state rather than rely on page-level HTML checksum fan-out.

## Why This Is Preferred

- stable even when bundle hashes drift
- easy to scan in Feishu
- gives operators direct click targets
- preserves the useful `next` signal without exposing low-value version detail
- removes misleading page-count noise

## Out of Scope

- bringing back URL-level change lists
- distinguishing `3.3.6`, `cloud`, or other non-`3.0` branches in the success message
- changing failure/baseline wording
