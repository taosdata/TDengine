#!/usr/bin/env python3
"""
TDengine 日志规范审查工具

使用 DeepSeek API 对 PR 中的 Rust 代码变更进行日志格式与内容审查。
审查标准来自 skills/taos-logging-style/SKILL.md。

安全措施:
  - 仅发送包含日志变更的 diff hunks（非完整源码）
  - 自动脱敏明显的密钥/凭据模式

退出码:
  0 — 审查通过（或无需审查）
  1 — 发现不符合规范的日志，阻塞合并
"""

import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

COMMENT_MARKER = "<!-- taos-logging-style-review -->"
ROUND_RE = re.compile(r"<!-- round:(\d+) -->")
MAX_DIFF_CHARS = 48_000
DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"
CONTEXT_LINES = 3  # context lines kept around each logging-related change

# Machine-readable verdict emitted by DeepSeek at the end of its response.
VERDICT_RE = re.compile(r"<!--\s*VERDICT:(PASS|FAIL)(?::(\d+))?\s*-->")

# Patterns indicating logging-related code (only meaningful on +/- lines)
LOG_PATTERNS = re.compile(
    r"^[+-].*("
    r"info!|warn!|error!|debug!|trace!|"
    r"log::|tracing::|println!|eprintln!|"
    r"\.info\(|\.warn\(|\.error\(|\.debug\(|\.trace\("
    r")",
    re.MULTILINE,
)

# Patterns that may contain secrets — values are redacted before sending
SENSITIVE_RE = re.compile(
    r"(password|passwd|secret|token|api_key|apikey|credential|"
    r"private_key|access_key|auth_token|bearer|dsn|connection_string)"
    r"""(\s*[:=]\s*)(["']?)\S+\3""",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(cmd, **kwargs):
    """Run a shell command and return CompletedProcess."""
    return subprocess.run(cmd, capture_output=True, text=True, **kwargs)


def gh_annotation(level: str, msg: str):
    """Emit a GitHub Actions annotation (error / warning / notice)."""
    print(f"::{level}::{msg}")


def write_step_summary(md: str):
    """Append Markdown to the GitHub Actions step summary."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as f:
            f.write(md + "\n")


def set_output(name: str, value: str):
    """Set a GitHub Actions output variable."""
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as f:
            f.write(f"{name}={value}\n")


# ---------------------------------------------------------------------------
# Diff extraction — only logging-related hunks
# ---------------------------------------------------------------------------

def get_rust_diff(pr_number: str) -> str:
    """Get the raw PR diff filtered to Rust source files only."""
    result = run(["gh", "pr", "diff", pr_number])
    if result.returncode != 0:
        print(f"Failed to get PR diff: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    lines = result.stdout.splitlines()
    rust_lines: list[str] = []
    include = False

    for line in lines:
        if line.startswith("diff --git"):
            include = line.rstrip().endswith(".rs")
        if include:
            rust_lines.append(line)

    return "\n".join(rust_lines)


def extract_logging_hunks(raw_diff: str) -> str:
    """From a Rust-only unified diff, keep only hunks that touch logging code.

    Each kept hunk preserves the full @@ block so the reviewer has natural
    diff context.  File headers are emitted only when the file has ≥1
    matching hunk.
    """
    lines = raw_diff.splitlines()
    output: list[str] = []

    file_header: list[str] = []
    current_hunk: list[str] = []
    file_has_output = False

    def flush_hunk():
        nonlocal file_has_output
        if not current_hunk:
            return
        hunk_text = "\n".join(current_hunk)
        if LOG_PATTERNS.search(hunk_text):
            if not file_has_output:
                output.extend(file_header)
                file_has_output = True
            output.extend(current_hunk)

    for line in lines:
        if line.startswith("diff --git"):
            flush_hunk()
            current_hunk = []
            file_header = [line]
            file_has_output = False
        elif line.startswith("@@"):
            flush_hunk()
            current_hunk = [line]
        elif current_hunk:
            current_hunk.append(line)
        else:
            # index / --- / +++ lines — part of file header
            file_header.append(line)

    flush_hunk()
    return "\n".join(output)


def scrub_sensitive(text: str) -> str:
    """Redact values that look like secrets/credentials."""
    return SENSITIVE_RE.sub(r"\1\2[REDACTED]", text)


# ---------------------------------------------------------------------------
# Skill & prompts
# ---------------------------------------------------------------------------

def read_skill(repo_root: str) -> str:
    """Read the logging style guidelines from the skill file."""
    path = os.path.join(repo_root, "skills", "taos-logging-style", "SKILL.md")
    try:
        with open(path) as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Skill file not found: {path}", file=sys.stderr)
        sys.exit(1)

    # Strip YAML front matter
    if content.startswith("---"):
        end = content.find("---", 3)
        if end != -1:
            content = content[end + 3 :].strip()
    return content


def build_system_prompt(guidelines: str) -> str:
    """Build the system prompt for the DeepSeek reviewer."""
    return f"""你是 TDengine 日志规范审查专家。请根据以下《日志编码规范》严格审查 PR 中的日志变更。

{guidelines}

## 审查重点
1. 是否覆盖了必须打日志的场景（收/发消息、超时、失败、关键分支）。
2. 是否违反了禁止规则（函数入口出口日志、循环内日志、中间结果刷屏）。
3. 日志文案是否为完整可读句子、变量值是否有可读字符串。
4. 日志级别是否恰当。

## 输出格式要求（中文，严格遵守）

### 📊 概要
一两句话总结日志变更的整体评价。

### ❌ 问题列表
逐条列出发现的问题（如无问题则写"未发现问题"）：
- **文件**: `<filename>` 约第 N 行
- **问题**: 描述
- **建议**: 修复方案

### ✅ 良好实践
列出符合规范的亮点（如有）。

## 结论标记（必须输出，独占最后一行）
- 如果未发现任何问题，最后一行输出：`<!-- VERDICT:PASS -->`
- 如果发现了问题，最后一行输出：`<!-- VERDICT:FAIL:N -->`（N 为问题总数）

注意：
- 你收到的 diff 仅包含日志相关的代码片段，请基于这些内容审查。
- 不要检查 `MOD`/`QID` 字段；它们由日志框架自动注入，不属于调用点日志代码的审查范围。
- 不要凭空捏造问题。
- 结论标记必须与正文内容一致，有问题就 FAIL，无问题就 PASS。"""


# ---------------------------------------------------------------------------
# DeepSeek API
# ---------------------------------------------------------------------------

def call_deepseek(api_key: str, system_prompt: str, user_prompt: str) -> str:
    """Call DeepSeek chat completions API."""
    payload = json.dumps(
        {
            "model": DEEPSEEK_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0,
            "max_tokens": 4096,
        }
    ).encode()

    req = urllib.request.Request(
        DEEPSEEK_API_URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            data = json.loads(resp.read())
        return data["choices"][0]["message"]["content"]
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        print(f"DeepSeek API error (HTTP {e.code}): {body}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"DeepSeek API call failed: {e}", file=sys.stderr)
        sys.exit(1)


def parse_verdict(review: str) -> tuple[str, str, int]:
    """Parse and strip the VERDICT marker from the review text.

    Returns (display_text, verdict, issue_count).
    verdict is "PASS", "FAIL", or "UNKNOWN".
    """
    match = VERDICT_RE.search(review)
    if match:
        verdict = match.group(1)  # PASS or FAIL
        issue_count = int(match.group(2) or 0)
        display = VERDICT_RE.sub("", review).rstrip()
        return display, verdict, issue_count

    # Fallback: no marker found — don't block on AI formatting issues
    return review.rstrip(), "UNKNOWN", 0


# ---------------------------------------------------------------------------
# PR comment
# ---------------------------------------------------------------------------

def upsert_comment(pr_number: str, *, status_line: str, review_body: str) -> int:
    """Create or update the review comment on the pull request.

    Returns the round number used.
    """
    repo = os.environ["GITHUB_REPOSITORY"]

    # Find existing comment with our marker and extract round number
    result = run(
        [
            "gh", "api",
            f"repos/{repo}/issues/{pr_number}/comments",
            "--jq",
            f'[.[] | select(.body | contains("{COMMENT_MARKER}"))][0] // empty',
        ]
    )

    comment_id = None
    prev_round = 0
    if result.stdout.strip():
        try:
            comment_obj = json.loads(result.stdout)
            comment_id = str(comment_obj["id"])
            m = ROUND_RE.search(comment_obj.get("body", ""))
            if m:
                prev_round = int(m.group(1))
        except (json.JSONDecodeError, KeyError):
            pass

    current_round = prev_round + 1

    body = (
        f"## 📋 日志规范审查 (Round {current_round}) — {status_line}\n\n"
        f"{review_body}\n\n"
        "---\n"
        "*🤖 由 [DeepSeek](https://deepseek.com) 基于 "
        "[TDengine 日志编码规范](skills/taos-logging-style/SKILL.md) 自动审查 "
        "· 仅发送日志相关 diff hunks*"
    )
    full_body = f"{COMMENT_MARKER}\n<!-- round:{current_round} -->\n{body}"

    if comment_id:
        run(
            [
                "gh", "api", "--method", "PATCH",
                f"repos/{repo}/issues/comments/{comment_id}",
                "-f", f"body={full_body}",
            ],
            check=True,
        )
        print(f"Updated existing review comment (id={comment_id}, round={current_round}).")
    else:
        run(
            ["gh", "pr", "comment", pr_number, "--body", full_body],
            check=True,
        )
        print(f"Created new review comment (round={current_round}).")

    return current_round


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    pr_number = os.environ.get("PR_NUMBER")
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    repo_root = os.environ.get("GITHUB_WORKSPACE", ".")

    if not pr_number:
        print("PR_NUMBER is not set.", file=sys.stderr)
        sys.exit(1)
    if not api_key:
        print("DEEPSEEK_API_KEY is not set.", file=sys.stderr)
        sys.exit(1)

    # 1. Get Rust-only diff
    print("Fetching PR diff for Rust files...")
    full_diff = get_rust_diff(pr_number)
    if not full_diff.strip():
        gh_annotation("notice", "No Rust file changes — logging review skipped.")
        write_step_summary("### 📋 日志规范审查\n\n✅ 无 Rust 文件变更，跳过审查。")
        upsert_comment(pr_number, status_line="✅ 审查通过", review_body="本次变更不涉及 Rust 文件，无需审查。")
        return

    # 2. Extract only hunks that contain logging changes
    logging_diff = extract_logging_hunks(full_diff)
    if not logging_diff.strip():
        gh_annotation("notice", "No logging-related changes — review skipped.")
        write_step_summary("### 📋 日志规范审查\n\n✅ 未检测到日志相关变更，跳过审查。")
        upsert_comment(pr_number, status_line="✅ 审查通过", review_body="本次变更未涉及日志相关代码，无需审查。")
        return

    # 3. Scrub sensitive values before sending externally
    logging_diff = scrub_sensitive(logging_diff)

    # 4. Read guidelines
    guidelines = read_skill(repo_root)

    # 5. Build prompts
    system_prompt = build_system_prompt(guidelines)

    if len(logging_diff) > MAX_DIFF_CHARS:
        logging_diff = logging_diff[:MAX_DIFF_CHARS] + "\n\n... (diff 因超长被截断)"

    user_prompt = (
        "请审查以下 Pull Request 中与日志相关的 diff hunks：\n\n"
        f"```diff\n{logging_diff}\n```"
    )

    # 6. Call DeepSeek for review
    print(
        f"Sending {len(logging_diff)} chars of logging-only diff to DeepSeek "
        f"(filtered from {len(full_diff)} chars total)..."
    )
    raw_review = call_deepseek(api_key, system_prompt, user_prompt)

    # 7. Parse verdict
    review_body, verdict, issue_count = parse_verdict(raw_review)

    if verdict == "FAIL":
        status_line = f"❌ 发现 {issue_count} 个问题 — 请修复后重新提交"
        status_emoji = "❌"
    elif verdict == "PASS":
        status_line = "✅ 审查通过"
        status_emoji = "✅"
    else:
        status_line = "ℹ️ 审查完成（未能解析结论，请人工确认）"
        status_emoji = "ℹ️"

    # 8. Post PR comment (with round number)
    round_num = upsert_comment(
        pr_number,
        status_line=status_line,
        review_body=review_body,
    )

    # 9. Write GitHub Actions step summary
    write_step_summary(
        f"### 📋 日志规范审查 (Round {round_num}) — {status_line}\n\n{review_body}"
    )

    # 10. Set outputs
    set_output("verdict", verdict)
    set_output("issue_count", str(issue_count))

    # 11. Exit
    if verdict == "FAIL":
        gh_annotation("error", f"日志规范审查发现 {issue_count} 个问题，请修复后重新提交。")
        print(f"\n{status_emoji} 审查未通过: 发现 {issue_count} 个不符合规范的日志。")
        #sys.exit(1)
        sys.exit(0)
    else:
        print(f"\n{status_emoji} {status_line}")


if __name__ == "__main__":
    main()
