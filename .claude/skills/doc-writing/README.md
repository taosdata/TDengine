# doc-writing

Write and review TDengine Markdown documentation following project CI standards.

## Description

This skill helps AI agents write high-quality Markdown documentation that passes all CI checks:
- markdownlint (format validation)
- typos (spell checking)
- AutoCorrect (Chinese formatting)

## Files

- `SKILL.md` - Main skill instructions (Agent Skills standard format)
- `CHEATSHEET.md` - Quick reference for common scenarios
- `scripts/check-local.sh` - Local validation script

## Usage

### Claude Code
```text
/doc-writing
```

### Other AI Agents
Read `SKILL.md` as context when working with documentation.

### Manual Validation
```bash
bash .claude/skills/doc-writing/scripts/check-local.sh
```

## Key Rules

1. **Code blocks**: Use ``` backticks with language specification (including `text` for plain text)
2. **Links**: Use `[text](url)`, never `<url>`
3. **Bold punctuation**: `**Bold:** text` (space required)
4. **Chinese-English**: Add spaces between Chinese and English/numbers
5. **Headings**: ATX style (#) with space after

## CI Integration

This skill aligns with `.github/workflows/tdengine-docs-ci.yml` which runs:
- markdownlint-cli2 (config: `docs/.markdownlint-cli2.jsonc`)
- typos (config: `docs/typos.toml`)
- AutoCorrect

### CI Check Scope

| Check | Scope |
|-------|-------|
| markdownlint | All changed `**/*.md` files |
| typos | `docs/zh/`, `docs/en/`, `./*.md` |
| AutoCorrect | `docs/zh/*`, `docs/en/*` |

## References

- Project CI workflow: `.github/workflows/tdengine-docs-ci.yml`
- Markdownlint config: `docs/.markdownlint-cli2.jsonc`
- Custom rules: `docs/custom-rules/`
