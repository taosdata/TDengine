#!/bin/bash

# 配置参数
REPOS=(
  "/home/TDinternal/"    # TDinternal 仓库的绝对路径
  "/home/TDinternal/community/"     # TDengine 仓库的绝对路径
)
SOURCE_BRANCH="3.0"             # 源分支
TARGET_BRANCH="cover/3.0"       # 目标分支
TIMEZONE="Asia/Shanghai"        # 时区（按需调整）

# 设置 Git 用户信息（避免提交失败）
export GIT_AUTHOR_EMAIL="happy_guoxy@163.com"
export GIT_AUTHOR_NAME="happyguoxy"
export GIT_COMMITTER_EMAIL="happy_guoxy@163.com"
export GIT_COMMITTER_NAME="happyguoxy"

# 获取当前时间（带时区）
TIMESTAMP=$(TZ=$TIMEZONE date +"%Y-%m-%d %H:%M")

# 同步每个仓库
for repo in "${REPOS[@]}"; do
  echo "Processing repository: $(basename $repo)"
  cd "$repo" || { echo "Directory not found: $repo"; exit 1; }

  # 拉取最新代码
  git fetch origin

  # 同步源分支到目标分支（快进合并）
  git checkout "$TARGET_BRANCH" || { echo "Failed to checkout $TARGET_BRANCH"; exit 1; }
  git pull origin "$TARGET_BRANCH"  # 确保目标分支最新
  git merge "$SOURCE_BRANCH" --ff-only || {
    echo "Fast-forward merge failed. Trying rebase..."
    git rebase "$SOURCE_BRANCH" || { echo "Rebase failed. Aborting."; exit 1; }
  }

  # 推送同步内容到远端分支
  git push origin "$TARGET_BRANCH" || { echo "Push failed"; exit 1; }

  # 推送空提交（记录同步时间）
  git commit --allow-empty -m "Sync branches at ${TIMESTAMP}" || echo "No changes to commit."
  git push origin "$TARGET_BRANCH" || { echo "Push failed"; exit 1; }

  echo "Repository $repo synchronized successfully."
done
