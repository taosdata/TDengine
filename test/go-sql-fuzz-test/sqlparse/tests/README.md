# Tests Directory

This directory contains external black-box tests (`package sqlparser_test`) that validate behavior through the public API.

## Scope

- End-to-end parse/format/walk behavior checks.
- Gate-style tests that should remain stable across refactors.
- Cross-package compatibility checks using only exported symbols.

## File Layout

- `operator_coverage_test.go`: real-SQL operator coverage gate. It verifies every defined expression operator is triggered by actual SQL parsing.

## Migration Rule

When adding new high-level gate tests, prefer placing them here if they can be written with exported API only.
Tests that require unexported internals should stay in repository root with `package sqlparser`.
