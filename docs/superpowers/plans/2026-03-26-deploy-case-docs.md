# Deploy Case Docs — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `deploy-case-docs.yml` GitHub Actions workflow that automatically generates versioned MkDocs documentation for all taosx test cases (Python E2E, Playwright TypeScript, Rust integration) and deploys them to gh-pages on PR merge.

**Architecture:** A Python generate script scans all three test directories and produces plain markdown files under `tests/e2e/docs/`, then auto-updates the `mkdocs.yml` nav. A GitHub Actions workflow triggers on PR merge and runs the script + `mike deploy` to push versioned docs to gh-pages.

**Tech Stack:** Python 3.9 (ast, re, pathlib, yaml), MkDocs + mkdocs-material + mkdocstrings + mike, GitHub Actions

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `.github/scripts/generate_case_md.py` | **Create** | Scans all 3 test dirs, generates markdown, updates mkdocs.yml nav |
| `.github/workflows/deploy-case-docs.yml` | **Create** | CI workflow: trigger, generate, mike deploy |
| `tests/e2e/mkdocs.yml` | **Modify** | Add `docs_dir`, `mike` plugin, initial `nav:` skeleton |
| `tests/e2e/docs/playwright_test_cases/` | **Create (dir)** | Generated Playwright test markdown files |
| `tests/e2e/docs/integration_test_cases/` | **Create (dir)** | Generated Rust integration test markdown files |

---

## Task 1: Update `tests/e2e/mkdocs.yml`

Add `docs_dir`, `mike` versioning plugin, and a base `nav:` skeleton. The generate script will overwrite the nav entries on each run, so the skeleton just needs the structure.

**Files:**
- Modify: `tests/e2e/mkdocs.yml`

- [ ] **Step 1: Open and review current mkdocs.yml**

```bash
cat tests/e2e/mkdocs.yml
```

Expected content (current):
```yaml
site_name: TaosX Cases Docs
theme:
  name: "material"

plugins:
  - search
  - mkdocstrings
```

- [ ] **Step 2: Update mkdocs.yml**

Replace the entire file with:

```yaml
site_name: TaosX Cases Docs
docs_dir: docs

theme:
  name: "material"

plugins:
  - search
  - mkdocstrings
  - mike

nav:
  - Home: index.md
  - E2E Test Cases: []
  - Playwright Test Cases: []
  - Integration Test Cases: []
```

- [ ] **Step 3: Verify mkdocs can parse the file**

```bash
cd tests/e2e && python3 -c "import yaml; yaml.safe_load(open('mkdocs.yml'))" && echo "OK"
```

Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/mkdocs.yml
git commit -m "chore: update mkdocs.yml with mike plugin and nav skeleton"
```

---

## Task 2: Create `.github/scripts/generate_case_md.py`

The script has three generators (Python, TypeScript, Rust) and one nav updater. Run from the repo root.

**Files:**
- Create: `.github/scripts/generate_case_md.py`

- [ ] **Step 1: Create the scripts directory**

```bash
mkdir -p .github/scripts
```

- [ ] **Step 2: Create `.github/scripts/generate_case_md.py`**

```python
#!/usr/bin/env python3
"""
Generate markdown documentation for taosx test cases.

Scans three test directories and produces plain markdown under tests/e2e/docs/:
  - tests/e2e/test_function/*_test.py  -> docs/e2e_test_cases/<name>.md
  - explorer/tests/*.spec.ts           -> docs/playwright_test_cases/<name>.md
  - tests/integration/**/*.rs          -> docs/integration_test_cases/<name>.md

Also rewrites the nav: section in tests/e2e/mkdocs.yml.

Usage:
    python .github/scripts/generate_case_md.py
"""

import ast
import os
import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).parent.parent.parent
MKDOCS_YML = REPO_ROOT / "tests" / "e2e" / "mkdocs.yml"
DOCS_DIR = REPO_ROOT / "tests" / "e2e" / "docs"

E2E_SRC = REPO_ROOT / "tests" / "e2e" / "test_function"
PLAYWRIGHT_SRC = REPO_ROOT / "explorer" / "tests"
INTEGRATION_SRC = REPO_ROOT / "tests" / "integration"

E2E_DOCS = DOCS_DIR / "e2e_test_cases"
PLAYWRIGHT_DOCS = DOCS_DIR / "playwright_test_cases"
INTEGRATION_DOCS = DOCS_DIR / "integration_test_cases"


# ---------------------------------------------------------------------------
# Python E2E generator
# ---------------------------------------------------------------------------

def generate_e2e_docs() -> list[str]:
    """Generate markdown for Python e2e test files. Returns list of nav entries."""
    E2E_DOCS.mkdir(parents=True, exist_ok=True)
    nav_entries = []

    for py_file in sorted(E2E_SRC.glob("*_test.py")):
        module_name = py_file.stem  # e.g. "kafka_test"
        tests = _extract_python_tests(py_file)
        if not tests:
            print(f"  [e2e] no test_ functions found in {py_file.name}, skipping")
            continue

        md_path = E2E_DOCS / f"{module_name}.md"
        lines = [f"# {module_name}\n\n"]
        for name, docstring in tests:
            lines.append(f"## {name}\n\n")
            if docstring:
                lines.append(f"{docstring.strip()}\n\n")
            else:
                lines.append("_No description._\n\n")
        md_path.write_text("".join(lines), encoding="utf-8")
        print(f"  [e2e] wrote {md_path.relative_to(REPO_ROOT)}")
        nav_entries.append({module_name: f"e2e_test_cases/{module_name}.md"})

    return nav_entries


def _extract_python_tests(file_path: Path) -> list[tuple[str, str | None]]:
    """Return list of (function_name, docstring_or_None) for test_ functions."""
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except Exception as exc:
        print(f"  [e2e] WARNING: failed to parse {file_path.name}: {exc}")
        return []

    results = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
            results.append((node.name, ast.get_docstring(node)))
    return results


# ---------------------------------------------------------------------------
# Playwright TypeScript generator
# ---------------------------------------------------------------------------

def generate_playwright_docs() -> list[str]:
    """Generate markdown for Playwright .spec.ts files. Returns list of nav entries."""
    PLAYWRIGHT_DOCS.mkdir(parents=True, exist_ok=True)
    nav_entries = []

    for ts_file in sorted(PLAYWRIGHT_SRC.glob("*.spec.ts")):
        stem = ts_file.stem  # e.g. "mqtt-task.spec" -> need to strip .spec
        name = stem.replace(".spec", "")
        groups = _extract_playwright_tests(ts_file)
        if not groups:
            print(f"  [playwright] no tests found in {ts_file.name}, skipping")
            continue

        md_path = PLAYWRIGHT_DOCS / f"{name}.md"
        lines = [f"# {name}\n\n"]
        for describe_title, test_list in groups:
            for test_title, comment in test_list:
                lines.append(f"## {describe_title} > {test_title}\n\n")
                if comment:
                    lines.append(f"{comment.strip()}\n\n")
        md_path.write_text("".join(lines), encoding="utf-8")
        print(f"  [playwright] wrote {md_path.relative_to(REPO_ROOT)}")
        nav_entries.append({name: f"playwright_test_cases/{name}.md"})

    return nav_entries


def _extract_playwright_tests(file_path: Path) -> list[tuple[str, list[tuple[str, str]]]]:
    """
    Parse a .spec.ts file and return list of (describe_title, [(test_title, comment)]).
    Handles single-level test.describe blocks. Tests outside a describe block are
    grouped under a synthetic describe named after the file stem.
    """
    source = file_path.read_text(encoding="utf-8")
    groups: list[tuple[str, list[tuple[str, str]]]] = []
    current_describe = file_path.stem.replace(".spec", "")
    current_tests: list[tuple[str, str]] = []

    lines = source.splitlines()
    i = 0
    pending_comment_lines: list[str] = []

    while i < len(lines):
        line = lines[i]

        # Accumulate single-line comments as potential test description
        stripped = line.strip()
        if stripped.startswith("//"):
            pending_comment_lines.append(stripped.lstrip("/ ").strip())
            i += 1
            continue

        # Match test.describe('Title', ...)
        m_describe = re.match(r"\s*test\.describe\(['\"](.+?)['\"]", line)
        if m_describe:
            if current_tests:
                groups.append((current_describe, current_tests))
            current_describe = m_describe.group(1)
            current_tests = []
            pending_comment_lines = []
            i += 1
            continue

        # Match test('Title', ...) or test("Title", ...)
        m_test = re.match(r"\s*test\(['\"](.+?)['\"]", line)
        if m_test:
            title = m_test.group(1)
            comment = " ".join(pending_comment_lines) if pending_comment_lines else ""
            current_tests.append((title, comment))
            pending_comment_lines = []
            i += 1
            continue

        # Non-comment, non-test line resets pending comments
        if stripped and not stripped.startswith("//"):
            pending_comment_lines = []

        i += 1

    if current_tests:
        groups.append((current_describe, current_tests))

    return groups


# ---------------------------------------------------------------------------
# Rust integration test generator
# ---------------------------------------------------------------------------

def generate_integration_docs() -> list[str]:
    """Generate markdown for Rust integration test files. Returns list of nav entries."""
    INTEGRATION_DOCS.mkdir(parents=True, exist_ok=True)
    nav_entries = []

    for rs_file in sorted(INTEGRATION_SRC.rglob("*.rs")):
        # Skip mod.rs and helper files - focus on datasource/core test files
        if rs_file.name in ("mod.rs", "lib.rs", "helpers.rs"):
            continue
        tests = _extract_rust_tests(rs_file)
        if not tests:
            continue

        # Use relative path components to build a unique name
        rel = rs_file.relative_to(INTEGRATION_SRC)
        name = "_".join(rel.with_suffix("").parts)  # e.g. "datasources_kafka"
        md_path = INTEGRATION_DOCS / f"{name}.md"
        lines = [f"# {name}\n\n"]
        for fn_name, doc in tests:
            lines.append(f"## {fn_name}\n\n")
            if doc:
                lines.append(f"{doc.strip()}\n\n")
            else:
                lines.append("_No description._\n\n")
        md_path.write_text("".join(lines), encoding="utf-8")
        print(f"  [integration] wrote {md_path.relative_to(REPO_ROOT)}")
        nav_entries.append({name: f"integration_test_cases/{name}.md"})

    return nav_entries


def _extract_rust_tests(file_path: Path) -> list[tuple[str, str | None]]:
    """Return list of (fn_name, doc_comment_or_None) for #[test] / #[tokio::test] fns."""
    try:
        source = file_path.read_text(encoding="utf-8")
    except Exception as exc:
        print(f"  [integration] WARNING: failed to read {file_path.name}: {exc}")
        return []

    results = []
    lines = source.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Collect consecutive /// doc comment lines
        doc_lines: list[str] = []
        while line.startswith("///"):
            doc_lines.append(line.lstrip("/ ").strip())
            i += 1
            if i >= len(lines):
                break
            line = lines[i].strip()

        # Skip attributes like #[test], #[tokio::test], #[ignore = ...]
        while re.match(r"^#\[", line):
            i += 1
            if i >= len(lines):
                break
            line = lines[i].strip()

        # Match async fn test_* or fn test_*
        m = re.match(r"^(?:pub\s+)?(?:async\s+)?fn\s+(test_\w+)", line)
        if m:
            fn_name = m.group(1)
            doc = " ".join(doc_lines) if doc_lines else None
            results.append((fn_name, doc))

        i += 1

    return results


# ---------------------------------------------------------------------------
# MkDocs nav updater
# ---------------------------------------------------------------------------

def update_mkdocs_nav(
    e2e_entries: list[str],
    playwright_entries: list[str],
    integration_entries: list[str],
) -> None:
    """Rewrite the nav: section in tests/e2e/mkdocs.yml."""
    with open(MKDOCS_YML, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    config["nav"] = [
        {"Home": "index.md"},
        {"E2E Test Cases": e2e_entries},
        {"Playwright Test Cases": playwright_entries},
        {"Integration Test Cases": integration_entries},
    ]

    with open(MKDOCS_YML, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    print(f"  [nav] updated {MKDOCS_YML.relative_to(REPO_ROOT)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("Generating E2E test docs...")
    e2e_nav = generate_e2e_docs()

    print("Generating Playwright test docs...")
    playwright_nav = generate_playwright_docs()

    print("Generating Integration test docs...")
    integration_nav = generate_integration_docs()

    print("Updating mkdocs.yml nav...")
    update_mkdocs_nav(e2e_nav, playwright_nav, integration_nav)

    total = len(e2e_nav) + len(playwright_nav) + len(integration_nav)
    print(f"\nDone. Generated {total} documentation pages.")
    print(f"  E2E:         {len(e2e_nav)}")
    print(f"  Playwright:  {len(playwright_nav)}")
    print(f"  Integration: {len(integration_nav)}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run the script locally to verify it works**

```bash
python .github/scripts/generate_case_md.py
```

Expected output: something like:
```
Generating E2E test docs...
  [e2e] wrote tests/e2e/docs/e2e_test_cases/kafka_test.md
  ...
Generating Playwright test docs...
  [playwright] wrote tests/e2e/docs/playwright_test_cases/mqtt-task.md
  ...
Generating Integration test docs...
  [integration] wrote tests/e2e/docs/integration_test_cases/datasources_kafka.md
  ...
Done. Generated N documentation pages.
```

- [ ] **Step 4: Verify the generated files look correct**

```bash
# Check a Python e2e doc
head -20 tests/e2e/docs/e2e_test_cases/kafka_test.md

# Check a Playwright doc
head -20 tests/e2e/docs/playwright_test_cases/mqtt-task.md

# Check a Rust integration doc
head -20 tests/e2e/docs/integration_test_cases/datasources_kafka.md

# Check mkdocs.yml nav was updated
grep -A 20 "^nav:" tests/e2e/mkdocs.yml
```

- [ ] **Step 5: Verify mkdocs can build the site locally (no errors)**

```bash
pip install mkdocs mkdocs-material mike "mkdocstrings[python]"
cd tests/e2e && mkdocs build --strict 2>&1 | tail -20
```

Expected: build completes without errors. Warnings about missing docstrings are acceptable.

- [ ] **Step 6: Commit**

```bash
git add .github/scripts/generate_case_md.py \
        tests/e2e/docs/ \
        tests/e2e/mkdocs.yml
git commit -m "feat: add generate_case_md.py script and regenerate docs"
```

---

## Task 3: Create `.github/workflows/deploy-case-docs.yml`

**Files:**
- Create: `.github/workflows/deploy-case-docs.yml`

- [ ] **Step 1: Create the workflow file**

```yaml
name: Deploy Case Docs

on:
  workflow_dispatch:
  pull_request:
    branches:
      - 'main'
    types: [closed]
    paths:
      - 'tests/e2e/test_function/*_test.py'
      - 'explorer/tests/*.spec.ts'
      - 'tests/integration/**/*.rs'

jobs:
  generate_and_deploy:
    if: github.event.pull_request.merged == true || github.event_name == 'workflow_dispatch'
    runs-on: ubuntu-latest

    permissions:
      contents: write

    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.9'

      - name: Install MkDocs and dependencies
        run: |
          pip install mkdocs mkdocs-material mike "mkdocstrings[python]" pyyaml

      - name: Set target branch name
        run: |
          if [ "${{ github.event_name }}" = "pull_request" ]; then
            BRANCH_NAME="${{ github.event.pull_request.base.ref }}"
          else
            BRANCH_NAME="${{ github.ref_name }}"
          fi
          echo "Target branch: $BRANCH_NAME"
          echo "BRANCH_NAME=$BRANCH_NAME" >> $GITHUB_ENV

      - name: Generate markdown documentation
        run: |
          python .github/scripts/generate_case_md.py
        shell: bash

      - name: Deploy documentation with Mike
        run: |
          git config --global user.name "GitHub Actions"
          git config --global user.email "actions@github.com"
          git fetch origin gh-pages || true
          cd tests/e2e
          mike deploy --push "$BRANCH_NAME" \
            --config-file mkdocs.yml \
            --branch gh-pages \
            --allow-empty
        shell: bash
```

- [ ] **Step 2: Verify the YAML is valid**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/deploy-case-docs.yml'))" && echo "YAML OK"
```

Expected: `YAML OK`

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/deploy-case-docs.yml
git commit -m "feat: add deploy-case-docs.yml CI workflow"
```

---

## Task 4: Smoke-test the full pipeline

Validate that everything works end-to-end before the first real PR triggers it.

- [ ] **Step 1: Run generate script from a clean state**

```bash
# Remove generated docs to simulate a fresh run
rm -rf tests/e2e/docs/e2e_test_cases \
       tests/e2e/docs/playwright_test_cases \
       tests/e2e/docs/integration_test_cases

python .github/scripts/generate_case_md.py
```

Expected: all three doc directories recreated, nav updated in mkdocs.yml.

- [ ] **Step 2: Run mkdocs build to confirm the site builds**

```bash
cd tests/e2e && mkdocs build 2>&1 | tail -10
```

Expected: `INFO  -  Documentation built in X.XX seconds`

- [ ] **Step 3: Verify workflow syntax with `act` (optional) or just lint the YAML**

```bash
python3 -c "
import yaml
wf = yaml.safe_load(open('.github/workflows/deploy-case-docs.yml'))
assert 'jobs' in wf
assert 'generate_and_deploy' in wf['jobs']
print('Workflow structure OK')
"
```

Expected: `Workflow structure OK`

- [ ] **Step 4: Restore generated docs and commit final state**

```bash
git add tests/e2e/docs/ tests/e2e/mkdocs.yml
git commit -m "chore: regenerate test case docs"
```

---

## Task 5: Final cleanup and push

- [ ] **Step 1: Review all changes**

```bash
git log --oneline -5
git diff origin/$(git branch --show-current)
```

- [ ] **Step 2: Push branch**

```bash
git push
```

- [ ] **Step 3: Manually trigger the workflow to verify end-to-end**

In GitHub UI: Actions → "Deploy Case Docs" → "Run workflow" → select branch `main`.

Monitor the run. Expected: all steps green, gh-pages branch updated with versioned docs at `/<branch>/`.
