---
name: testcase-authoring
description: Guidelines for writing efficient and reliable TDengine test cases. Use when creating or reviewing TDengine test cases, especially for timeout handling, batch inserts, DRY refactoring, and assertion-safe data volume changes.
---

# Test Case Authoring Guide

## Instructions

Use this skill when writing or reviewing TDengine test cases, especially for:
- timeout and retry logic
- batch insert patterns
- DRY refactoring of repeated logic
- safe data volume changes that keep assertions consistent

When applying guidance from this skill:
- preserve original test behavior first, then optimize
- prefer existing framework helpers over new custom loops
- keep examples executable and aligned with current framework APIs

## Workflow

1. Read the detailed guide in [REFERENCE.md](REFERENCE.md).
2. Identify the requested change category: timeout, batching, DRY refactor, or data volume.
3. Apply the relevant patterns from the reference with minimal behavior change.
4. Re-check assertion expectations after any data volume or SQL construction change.

## Quick Checklist

- [ ] Original behavior preserved?
- [ ] Existing helper APIs used before adding custom loops?
- [ ] Batch insert logic handles empty lists safely?
- [ ] Dynamic SQL values converted to valid SQL literals?
- [ ] Assertions updated when test data volume changes?

## Examples

Example requests that should trigger this skill:
- "Review this TDengine test for timeout and retry issues"
- "Refactor repeated insert logic in this test case"
- "Reduce test data volume from 100 to 50 without breaking assertions"