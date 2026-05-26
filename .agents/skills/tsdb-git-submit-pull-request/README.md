# tsdb-git-submit-pull-request Skill

Automatically create Pull Requests (GitHub) or Merge Requests (GitLab) based on a Feishu work item, across all git repositories in the workspace.

## Usage

Type `/tsdb-git-submit-pull-request` in the chat, or ask the agent to submit a PR/MR.

## Prerequisites

Three MCP Servers need to be configured. **Feishu** is required, and at least one of **GitHub** or **GitLab** must be configured.

### Feishu MCP Server

Used to query work item details and update the PR link field after submission.

Refer to: https://project.feishu.cn/b/mcp

### GitHub MCP Server

Used to create Pull Requests on GitHub repositories.

Refer to: https://github.com/github/github-mcp-server

### GitLab MCP Server

Used to create Merge Requests on GitLab repositories.

Refer to: https://docs.gitlab.com/user/gitlab_duo/model_context_protocol/mcp_server/

## What It Does

1. Asks for a Feishu work item ID (or URL) and queries its details (name, type, description, comments).
2. Asks for a target branch (default: `main`).
3. Discovers all git repositories in the workspace, filtering for branches with recognized prefixes (`fix`, `enh`, `feat`, `chore`, `docs`, `test`).
4. For each matching repository:
   - Pushes local commits to the remote.
   - Generates an English PR/MR title and description from commit messages, and code diff.
   - Creates a PR (GitHub) or MR (GitLab) via the corresponding MCP Server.
5. Updates the Feishu work item's "代码实现（PR）" field with links to all created PRs/MRs.
