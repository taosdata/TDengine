---
name: tsdb-git-submit-pull-request
description: "Automatically submit Pull Requests (GitHub) or Merge Requests (GitLab) based on a Feishu work item. Use when: creating PR, submit pr, submitting MR, 提交PR, 提交MR, submit-pull-request, submit pull request, submit merge request, 提交合并请求"
metadata: 
  author: "Bomin Zhang"
  version: 1.0.0
  owner_team: engine
---

# Submit Pull Request / Merge Request

Automate the creation of Pull Requests (GitHub) or Merge Requests (GitLab) from Feishu work items across all git repositories in the workspace.

## Prerequisites

The user must have the following MCP Servers configured (names are case-insensitive, partial match):
- **Feishu**: For querying and updating work item information
- **GitHub** and/or **GitLab**: At least one must be configured for creating PRs/MRs

Use `tool_search_tool_regex` to discover available MCP tools:
- Pattern `mcp_feishu` for Feishu tools
- Pattern `mcp_io_github` for GitHub tools
- Pattern `mcp.*gitlab` for GitLab tools (exclude any tool containing `gitkraken` — do NOT use GitKraken MCP tools)

If Feishu MCP tools are not found, abort and inform the user.
If neither GitHub nor GitLab MCP tools are found, abort and inform the user.

## Input

Collect the following information from the user step by step in order. **Do not ask all questions at once**. Confirm each item before moving to the next one.

### Step 1: Collect Feishu Work Item ID

Ask the user for the Feishu work item ID (use `vscode_askQuestions` if available, otherwise prompt directly):
- Prompt: "请输入飞书工作项 ID 或链接（Please enter the Feishu work item ID or URL）"

Parse the input:
- If the input is a **number**, use it directly as the work item ID.
- If the input is a **URL**, extract the **last segment** of the URL path — it should be a numeric ID.

If the parsed result is not a valid number, ask again.

Record the work item ID for later use.

### Step 2: Ask for target branch

Ask the user for the target branch (use `vscode_askQuestions` if available, otherwise prompt directly):
- Prompt: "请输入 Pull Request 的目标分支（默认 main）/ Target branch for Pull Request (default: main)"
- Default value: `main`

If the user provides an empty answer, use `main`.

Record the target branch for later use.

## Procedure

### Step 1: Query Feishu Work Item

Use the Feishu MCP Server to query the work item in project space **taosdata_td**.

Use `mcp_feishu_get_workitem_brief` (or equivalent) with the work item ID and project key `taosdata_td` to retrieve:
- **Name** (工作项名称)
- **Type** (工作项类型: 任务/sub_task, Feature/feature, Defect/defect, Job/job)
- **Description** (描述)

Record all of these for later use.

### Step 2: Compute Work Item Link

Build the Feishu work item link using this template:

```
https://project.feishu.cn/taosdata_td/{type_slug}/detail/{work_item_id}
```

Map the work item type to `type_slug`:
| Work Item Type | type_slug |
|---------------|-----------|
| 任务 (Task / sub_task) | `sub_task` |
| Feature | `feature` |
| Defect | `defect` |
| Job | `job` |

### Step 3: Discover Git Repositories

The workspace may contain multiple git repositories (due to git submodules, etc.).

Run the following terminal command to find all `.git` directories, excluding any `contrib` folders, excluding any hidden folders (e.g., `.vscode`, `.github`), and only searching up to 3 levels deep:

```bash
find <workspace_dir> -maxdepth 3 -path '*/contrib/*' -prune -o -path '*/.*/*' -prune -o \( -name .git -type d -o -name .git -type f \) -print
```

For each discovered git repository, get the current branch name:

```bash
git -C <repo_path> rev-parse --abbrev-ref HEAD
```

**Filter**: A repository needs processing ONLY if its current branch name starts with one of these prefixes (case-insensitive):
- `fix`
- `enh`
- `feat`
- `chore`
- `docs`
- `test`

Repositories whose branch names do not match any of these prefixes are skipped.

### Step 4: Validate Branch Consistency

All repositories that passed the filter in Step 3 must have the **exact same branch name**.

- If they all match → proceed with all of them.
- If they differ → present the list to the user and ask which repositories to process (use `vscode_askQuestions` if available, otherwise prompt directly).

### Step 5: Process Each Repository

For each repository that needs processing, perform the following sub-steps:

#### Step 5a: Push Local Commits

Ensure all local commits are pushed to the remote:

```bash
git -C <repo_path> status
git -C <repo_path> push
```

If push fails, inform the user and ask how to proceed.

#### Step 5b: Generate PR/MR Description

First, get the diff summary against the target branch:

```bash
git -C <repo_path> log --oneline <target_branch>..HEAD
git -C <repo_path> diff <target_branch>..HEAD --stat
```

Using the **commit messages** and the **code diff summary**, generate the PR/MR title and description in **English**. Do NOT include work item details (name, description, comments) in the generated title or description — they contain sensitive information.

**Generate PR/MR Title:**

Generate a concise English title summarizing the changes based on the commit messages and code diff summary. Then add a prefix according to the following rules (evaluated in priority order, use the first matching rule):

1. If the work item type is **Defect** → prefix is `fix: `
2. If the current branch name starts with `enh` (case-insensitive) → prefix is `enh: `
3. If the work item type is **Feature** → prefix is `feat: `
4. If the code changes are **documentation-only** (e.g., only `docs/`, `*.md` files changed) → prefix is `docs: `
5. If the code changes are **test-only** (e.g., only `test/`, `tests/` files changed) → prefix is `test: `
6. Otherwise → prefix is `enh: `

The final title format is: `{prefix}{generated_title}`

**Generate PR/MR Description:**

Follow this exact template:

```markdown
# Description

<!-- Write a clear description summarizing the changes based on the work item info and code diff -->
{generated_description}

# Issue(s)

- Close: {feishu_work_item_link}

# Checklist

Please check the items in the checklist if applicable.

- [ ] Is the user manual updated?
- [ ] Are the test cases passed and automated?
- [ ] Is there no significant decrease in test coverage?
```

Replace `{generated_description}` with a meaningful English description synthesized from the commit messages and code changes.
Replace `{feishu_work_item_link}` with the link computed in Step 2.

#### Step 5c: Determine Remote Type and Create PR/MR

Get the remote URL for the repository:

```bash
git -C <repo_path> remote get-url origin
```

- If the remote URL contains `github.com` → use the **GitHub** MCP Server to create a Pull Request.
- If the remote URL contains `gitlab` → use the **GitLab** MCP Server to create a Merge Request.

**For GitHub PRs:**
Use the GitHub MCP tool to create a pull request. You'll need:
- `owner` and `repo` parsed from the remote URL
- `title`: the generated title from Step 5b
- `body`: the generated description from Step 5b
- `head`: the current branch name
- `base`: the target branch from Step 2 of the Input section

**For GitLab MRs:**
Use the GitLab MCP tool to create a merge request. You'll need:
- `project`: parsed from the remote URL (usually `namespace/project`)
- `title`: the generated title from Step 5b
- `description`: the generated description from Step 5b
- `source_branch`: the current branch name
- `target_branch`: the target branch from Step 2 of the Input section

Record the URL of each created PR/MR.

### Step 6: Update Feishu Work Item

<!-- NOTE: Currently the PR link gets updated to the "Pull Request" tab instead of the "代码实现（PR）" field on the "处理过程" tab. This may be a Feishu MCP issue. -->

After all PRs/MRs are created, update the Feishu work item's **代码实现（PR）** field.

Use the Feishu MCP Server to **append** all created PR/MR links to the field. Do NOT overwrite existing values — append to them.

Format each link on its own line.

## Output: Report Results

Summarize the results to the user:
- List each repository processed
- Show the PR/MR URL created for each
- Confirm the Feishu work item was updated

## Safety
- Never request or reveal secrets (tokens/passwords/keys).
- Never bypass failed checks; report failures explicitly.
- Never run destructive system commands while validating skills.
- Do not claim conflict/security checks were executed unless they were explicitly run as separate repository workflows.

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-git-submit-pull-request version=1.0.0 author=Bomin Zhang`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
