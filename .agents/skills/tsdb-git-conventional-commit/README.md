# tsdb-git-conventional-commit

An agent skill that generates [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/) compliant commit messages from `git diff` output and guides you through the commit and push flow.

## What it does

- Reads staged or unstaged diff live from git
- Selects the correct commit type (`feat`, `fix`, `docs`, `refactor`, etc.)
- Optionally adds a scope, breaking-change marker, `Closes` footer, and multi-line body
- Runs `git commit` and `git push` with your confirmation at each step
- **Oneline Mode** — outputs a single-line title and skips all interactive steps

## Trigger phrases

| Phrase | Behavior |
|--------|----------|
| "commit this", "help me commit", "generate a commit message" | Full interactive flow |
| "oneline", "one-line", "quick commit", "just the title" | Oneline Mode (title only) |

## Commit format

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Supported types

| Type | When to use |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `style` | Formatting, no logic change |
| `refactor` | Code restructure, not feat/fix |
| `perf` | Performance improvement |
| `test` | Adding or updating tests |
| `build` | Build system / dependency changes |
| `ci` | CI configuration changes |
| `chore` | Other non-src/test changes |
| `revert` | Reverting a previous commit |

Append `!` after type/scope for breaking changes, e.g. `feat(api)!:`.

## Interactive flow

1. Read `git diff --staged` (falls back to `git diff`)
2. Generate a commit title following the spec
3. Ask whether to add a `Closes` footer (issue/PR link)
4. Ask whether to add a multi-line body
5. Display the final message for review
6. Ask to run `git commit`
7. Ask to run `git push`

## Files

- `SKILL.md` — skill definition and full execution instructions
