---
name: tsdb-git-conventional-commit
description: Generate a Conventional Commits 1.0.0 compliant git commit message and execute commit and push operations. Trigger when the user wants to commit code, generate a commit message, write a git message, or push code — even for casual prompts like "commit this", "help me commit", or "generate a commit message". When the user says "oneline", "one-line", "one line", "quick commit", or "just the title", trigger Oneline Mode — skip the Closes and body steps and output only a single-line title.
metadata:
  author: Astro Yan
  version: 1.0.0
  owner_team: engine
---

# Conventional Commit Skill

Reads `git diff` output and generates a standard commit message following the [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/#specification) specification, then guides the user through the commit and push flow.

---

## Mode Detection: Oneline Quick Commit

Before executing any step, check whether the user has triggered **Oneline Mode**.

**Triggers** (any one of the following is sufficient):
- User explicitly says `oneline`, `one-line`, or `one line`
- User uses phrases like "quick commit", "fast commit", "just the title", "skip the details", "no body needed"
- User implies they want a minimal, no-frills commit message

**Oneline Mode behavior**:
- Execute Step 1 (read diff)
- Execute Step 2 (generate commit message — title only, no body, no Closes)
- **Skip Step 3 (Closes) and Step 4 (multiline body)**
- Jump directly to Step 5 to display the single-line commit message, then continue with Step 6 and Step 7

---

## Execution Flow

Follow these steps strictly in order. Complete each step before moving to the next.

### Step 1: Read git diff

**Do not rely on conversation history or previously seen code.** Always run the commands live to get the latest changes:

```bash
# Prefer staged changes
git diff --staged

# If staging area is empty, read working directory changes
git diff
```

If both are empty, inform the user that no changes were detected and suggest running `git add` to stage files first.

---

### Step 2: Generate a Conventional Commit message

Analyze the diff and generate a commit message following this spec:

#### Format

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

#### Type Selection

| Type | When to use |
|------|-------------|
| `feat` | New feature (SemVer MINOR) |
| `fix` | Bug fix (SemVer PATCH) |
| `docs` | Documentation changes only |
| `style` | Formatting changes that don't affect logic (whitespace, indentation, semicolons) |
| `refactor` | Code restructuring — neither a new feature nor a bug fix |
| `perf` | Performance improvements |
| `test` | Adding or updating tests |
| `build` | Build system or external dependency changes |
| `ci` | CI configuration changes |
| `chore` | Other changes that don't modify src or test files |
| `revert` | Reverting a previous commit |

#### Scope (optional)
- Wrap in parentheses to describe the affected module/component, e.g. `feat(auth):` / `fix(parser):`

#### Breaking Changes
- Append `!` after type/scope, e.g. `feat(api)!:`
- Or include `BREAKING CHANGE: <description>` in the footer

#### Description Rules
- Follows the colon and a space
- Use imperative mood, lowercase first letter, no trailing period
- Keep it concise — typically under 72 characters

---

### Step 3: Ask whether to add a Closes line

> ⚠️ **Skip this step in Oneline Mode.**

After generating the draft title, **proactively ask the user**:

> Do you want to add a `Closes` line below the title? If there's a related issue or PR link, please share it.

- If the user provides a Markdown link in `[xxx](xxx)` format, **use it directly as-is** without any modification:
  ```
  Closes: [the user's original markdown link]
  ```
  For example, if the user inputs `[fix login crash on iOS](https://github.com/org/repo/issues/42)`, output:
  ```
  Closes: [fix login crash on iOS](https://github.com/org/repo/issues/42)
  ```

- If the user provides a plain URL (not Markdown format), try to fetch the page to get its title or summary, then format it as:
  ```
  Closes: [short conclusion](link)
  ```
  where `short conclusion` is a brief description of the issue/PR (5 words or fewer). If the link is **inaccessible** (network error, 403, 404, etc.), use the last path segment of the URL as the link name, e.g.:
  - `https://github.com/org/repo/issues/42` → `[42](https://github.com/org/repo/issues/42)`
  - `https://jira.example.com/browse/PROJ-123` → `[PROJ-123](https://jira.example.com/browse/PROJ-123)`

- If the user only provides a number (e.g. `#42`), format it as:
  ```
  Closes: #42
  ```

- If the user doesn't need this, skip the step.

---

### Step 4: Ask whether to add a multi-line body

> ⚠️ **Skip this step in Oneline Mode.**

**Proactively ask the user**:

> Do you want to add a detailed multi-line body after the title?

- If yes, generate a body based on the diff that covers:
  - Background/motivation (Why)
  - What was changed (What)
  - Impact or scope, if relevant
- Separate the body from the title with a blank line; separate paragraphs with blank lines too.
- If the user says no (or declines in any way), **omit the body entirely** — the final commit message should contain only the title (and the `Closes` line if added in Step 3).

---

### Step 5: Display the final commit message

Show the complete commit message to the user in a clear code block, e.g.:

```
feat(auth): add OAuth2 login support

Closes: [add social login feature](https://github.com/org/repo/issues/88)

Implement Google and GitHub OAuth2 providers using passport.js.
Adds token refresh logic and secure session management.
```

Allow the user to request edits before proceeding.

---

### Step 6: Ask whether to run git commit

**Proactively ask the user**:

> Ready to run `git commit`?

- If the user agrees, execute:
  ```bash
  git commit -m "<title>" -m "<body (if any)>"
  ```
  For multi-paragraph bodies, use multiple `-m` flags or a heredoc:
  ```bash
  git commit -F- <<'EOF'
  <full commit message>
  EOF
  ```
- Display the git output after execution.
- If the user declines, output the final message for manual use and end the flow.

---

### Step 7: Ask whether to run git push

After a successful commit, **proactively ask the user**:

> Do you want to run `git push` to push this commit to the remote?

- If the user agrees, execute:
  ```bash
  git push
  ```
  If push fails (e.g. upstream not set), suggest:
  ```bash
  git push --set-upstream origin <branch-name>
  ```
- Display the push result.
- If the user declines, end the flow and confirm the commit is saved locally.

---

## Notes

- **Always read the diff live from git commands** — never rely on code seen earlier in the conversation.
- If the diff is very long, focus on the type of change and the core modifications; no need to describe every line.
- If a single diff contains multiple change types (e.g. both `feat` and `fix`), choose the type with greater impact and describe the others in the body.
- Commit messages should be in English by default, unless the user explicitly requests another language.
- Wait for user confirmation after each step — do not execute all steps at once.

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-git-conventional-commit version=0.1.0 author=Astro Yan`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
