#!/bin/bash
REPO_PATH=$1
TARGET_BRANCH=$2
PR_NUMBER=$3

cd $REPO_PATH
git reset --hard
git clean -f
git remote prune origin
git fetch
git checkout $TARGET_BRANCH
git pull >/dev/null
git fetch origin +refs/pull/$PR_NUMBER/merge
git checkout -qf FETCH_HEAD
