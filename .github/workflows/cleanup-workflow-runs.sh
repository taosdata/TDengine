#!/usr/bin/env bash

if [ $# -ne 2 ]; then
  echo -e "Usage:\n$0 <repo-name> <workflow-id>"
  exit 1
fi

REPO=$1
WORKFLOW_ID=$2

echo "REPO: $REPO"
echo "WORKFLOW_ID: $WORKFLOW_ID"

while true; do
  # the max number of runs per page is 100
  gh api "repos/taosdata/$REPO/actions/workflows/$WORKFLOW_ID/runs?per_page=100" | \
    jq '.workflow_runs[].id' > run-id.txt

  if [ $? -ne 0 ]; then
    echo "Failed to get the list of workflow runs"
    exit 1
  fi

  for id in $(cat run-id.txt); do
    echo $id && gh run delete --repo taosdata/$REPO $id
  done
done