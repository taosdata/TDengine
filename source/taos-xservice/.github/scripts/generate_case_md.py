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
import inspect
import re
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

def _humanize_case_name(raw: str) -> str:
    """Convert function/test title to readable phrase."""
    text = raw.replace(">", " ").replace("_", " ").replace("-", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _auto_case_description(case_name: str, scope: str) -> str:
    """Generate fallback bilingual description when source description is absent."""
    readable = _humanize_case_name(case_name)
    return (
        f"- **EN**: Validates the `{readable}` scenario in `{scope}` tests.\n"
        f"- **中文**：验证 `{readable}` 场景在 `{scope}` 测试中的行为与预期。"
    )


def _looks_like_code_only(text: str) -> bool:
    """Best-effort check for comment text that is actually disabled code."""
    t = text.strip()
    if not t:
        return False
    code_like_patterns = [
        r"^[A-Za-z_][\w.]*\s*\(",
        r"^\s*(if|for|while|return|const|let|var|await)\b",
        r"[;{}=>]",
        r"^\s*test\.",
        r"^\s*expect\(",
    ]
    return any(re.search(p, t) for p in code_like_patterns)


def _normalize_markdown_lines(lines: list) -> str:
    """Keep markdown line breaks and strip common comment prefixes."""
    cleaned = []
    for line in lines:
        value = line.rstrip()
        value = re.sub(r"^\s*\*\s?", "", value)
        cleaned.append(value)

    while cleaned and not cleaned[0].strip():
        cleaned.pop(0)
    while cleaned and not cleaned[-1].strip():
        cleaned.pop()
    normalized = []
    for idx, line in enumerate(cleaned):
        normalized.append(line)
        current = line.strip()
        next_line = cleaned[idx + 1].strip() if idx + 1 < len(cleaned) else ""
        is_section_label = bool(re.match(r"^(用例步骤|验证点|steps|step|validation|checks?)\s*[：:]$", current, re.IGNORECASE))
        next_is_list = bool(re.match(r"^(\d+\.|[-*+])\s+", next_line))
        if is_section_label and next_line and next_is_list:
            normalized.append("")
            continue
        current_is_paragraph = bool(current) and not bool(
            re.match(r"^(\d+\.|[-*+])\s+|^#{1,6}\s+|^```|^>|^\|", current)
        )
        if current_is_paragraph and next_line and next_is_list:
            normalized.append("")

    text = "\n".join(normalized).strip()
    if _looks_like_code_only(text):
        return ""
    return text


def _normalize_markdown_text(text: str) -> str:
    """Normalize free-form markdown text while preserving markdown structure."""
    if not text:
        return ""
    dedented = inspect.cleandoc(text)
    return _normalize_markdown_lines(dedented.splitlines())

def _reset_generated_markdown(target_dir: Path) -> None:
    """Remove existing generated markdown files to avoid stale pages."""
    if not target_dir.exists():
        return
    for md_file in target_dir.glob("*.md"):
        md_file.unlink()


# ---------------------------------------------------------------------------
# Python E2E generator
# ---------------------------------------------------------------------------

def generate_e2e_docs() -> list:
    """Generate markdown for Python e2e test files. Returns list of nav entries."""
    E2E_DOCS.mkdir(parents=True, exist_ok=True)
    _reset_generated_markdown(E2E_DOCS)
    nav_entries = []

    for py_file in sorted(E2E_SRC.glob("*_test.py")):
        module_name = py_file.stem
        tests = _extract_python_tests(py_file)
        if not tests:
            print(f"  [e2e] no test_ functions found in {py_file.name}, skipping")
            continue

        md_path = E2E_DOCS / f"{module_name}.md"
        lines = [f"# {module_name}\n\n"]
        for name, docstring in tests:
            lines.append(f"## {name}\n\n")
            normalized_doc = _normalize_markdown_text(docstring) if docstring else ""
            description = normalized_doc if normalized_doc else _auto_case_description(name, "Python E2E")
            lines.append(f"{description}\n\n")
        md_path.write_text("".join(lines), encoding="utf-8")
        print(f"  [e2e] wrote {md_path.relative_to(REPO_ROOT)}")
        nav_entries.append({module_name: f"e2e_test_cases/{module_name}.md"})

    return nav_entries


def _extract_python_tests(file_path: Path) -> list:
    """Return list of (function_name, docstring_or_None) for test_ functions, in source order.

    Handles the taosx pattern where docstrings appear after an early-return version
    check rather than as the first statement, so ast.get_docstring() misses them.
    Falls back to scanning the entire function body for the first string literal.
    """
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except Exception as exc:
        print(f"  [e2e] WARNING: failed to parse {file_path.name}: {exc}")
        return []

    results = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
            # Try standard docstring first (first statement is a string literal)
            doc = ast.get_docstring(node)
            if not doc:
                # Fallback: find first string literal anywhere in the function body
                for child in ast.walk(node):
                    if (
                        isinstance(child, ast.Expr)
                        and isinstance(child.value, ast.Constant)
                        and isinstance(child.value.value, str)
                    ):
                        doc = child.value.value.strip()
                        break
            results.append((node.lineno, node.name, doc))
    results.sort(key=lambda x: x[0])
    return [(name, doc) for _, name, doc in results]


# ---------------------------------------------------------------------------
# Playwright TypeScript generator
# ---------------------------------------------------------------------------

def generate_playwright_docs() -> list:
    """Generate markdown for Playwright .spec.ts files. Returns list of nav entries."""
    PLAYWRIGHT_DOCS.mkdir(parents=True, exist_ok=True)
    _reset_generated_markdown(PLAYWRIGHT_DOCS)
    nav_entries = []

    for ts_file in sorted(PLAYWRIGHT_SRC.glob("*.spec.ts")):
        stem = ts_file.stem
        name = stem.replace(".spec", "")
        groups = _extract_playwright_tests(ts_file)
        if not groups:
            print(f"  [playwright] no tests found in {ts_file.name}, skipping")
            continue

        md_path = PLAYWRIGHT_DOCS / f"{name}.md"
        lines = [f"# {name}\n\n"]
        for describe_title, test_list in groups:
            for test_title, comment in test_list:
                if describe_title != name:
                    lines.append(f"## {describe_title} > {test_title}\n\n")
                else:
                    lines.append(f"## {test_title}\n\n")
                case_key = f"{describe_title} > {test_title}" if describe_title != name else test_title
                cleaned_comment = comment.strip() if comment else ""
                description = cleaned_comment if cleaned_comment else _auto_case_description(case_key, "Playwright")
                lines.append(f"{description}\n\n")
        md_path.write_text("".join(lines), encoding="utf-8")
        print(f"  [playwright] wrote {md_path.relative_to(REPO_ROOT)}")
        nav_entries.append({name: f"playwright_test_cases/{name}.md"})

    return nav_entries


def _extract_playwright_tests(file_path: Path) -> list:
    """
    Parse a .spec.ts file and return list of (describe_title, [(test_title, comment)]).
    Tests outside a describe block are grouped under the file stem.
    """
    source = file_path.read_text(encoding="utf-8")
    groups = []
    current_describe = file_path.stem.replace(".spec", "")
    current_tests = []

    lines = source.splitlines()
    i = 0
    pending_comment_lines = []

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if stripped.startswith("//"):
            pending_comment_lines.append(stripped[2:].lstrip())
            i += 1
            continue
        if stripped.startswith("/*"):
            block = []
            if "*/" in stripped:
                block.append(stripped[2:].split("*/", 1)[0])
                pending_comment_lines.extend(block)
                i += 1
                continue
            block.append(stripped[2:])
            i += 1
            while i < len(lines):
                block_line = lines[i].strip()
                if "*/" in block_line:
                    block.append(block_line.split("*/", 1)[0])
                    i += 1
                    break
                block.append(block_line)
                i += 1
            pending_comment_lines.extend(block)
            continue

        m_describe = re.match(r"\s*test\.describe\(['\"`](.+?)['\"`]", line)
        if m_describe:
            if current_tests:
                groups.append((current_describe, current_tests))
            # Reset to current describe title to avoid runaway concatenation across sibling blocks.
            current_describe = m_describe.group(1)
            current_tests = []
            pending_comment_lines = []
            i += 1
            continue

        m_test = re.match(r"\s*test\(['\"`](.+?)['\"`]", line)
        if m_test:
            title = m_test.group(1)
            comment = _normalize_markdown_lines(pending_comment_lines) if pending_comment_lines else ""
            current_tests.append((title, comment))
            pending_comment_lines = []
            i += 1
            continue

        if stripped and not stripped.startswith("//"):
            pending_comment_lines = []

        i += 1

    if current_tests:
        groups.append((current_describe, current_tests))

    return groups


# ---------------------------------------------------------------------------
# Rust integration test generator
# ---------------------------------------------------------------------------

def generate_integration_docs() -> list:
    """Generate markdown for Rust integration test files. Returns list of nav entries."""
    INTEGRATION_DOCS.mkdir(parents=True, exist_ok=True)
    _reset_generated_markdown(INTEGRATION_DOCS)
    nav_entries = []

    for rs_file in sorted(INTEGRATION_SRC.rglob("*.rs")):
        if rs_file.name in ("mod.rs", "lib.rs", "helpers.rs", "fixtures.rs", "health_check.rs"):
            continue
        tests = _extract_rust_tests(rs_file)
        if not tests:
            continue

        rel = rs_file.relative_to(INTEGRATION_SRC)
        name = "_".join(rel.with_suffix("").parts)
        md_path = INTEGRATION_DOCS / f"{name}.md"
        lines = [f"# {name}\n\n"]
        for fn_name, doc in tests:
            lines.append(f"## {fn_name}\n\n")
            description = doc.strip() if doc else _auto_case_description(fn_name, "Rust integration")
            lines.append(f"{description}\n\n")
        md_path.write_text("".join(lines), encoding="utf-8")
        print(f"  [integration] wrote {md_path.relative_to(REPO_ROOT)}")
        nav_entries.append({name: f"integration_test_cases/{name}.md"})

    return nav_entries


def _extract_rust_tests(file_path: Path) -> list:
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

        doc_lines = []
        while line.startswith("///"):
            doc_lines.append(line.lstrip("/ ").strip())
            i += 1
            if i >= len(lines):
                break
            line = lines[i].strip()

        while re.match(r"^#\[", line):
            i += 1
            if i >= len(lines):
                break
            line = lines[i].strip()

        m = re.match(r"^(?:pub\s+)?(?:async\s+)?fn\s+(test_\w+)", line)
        if m:
            fn_name = m.group(1)
            doc = _normalize_markdown_lines(doc_lines) if doc_lines else None
            results.append((fn_name, doc))

        i += 1

    return results


# ---------------------------------------------------------------------------
# MkDocs nav updater
# ---------------------------------------------------------------------------

def update_mkdocs_nav(e2e_entries: list, playwright_entries: list, integration_entries: list) -> None:
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
