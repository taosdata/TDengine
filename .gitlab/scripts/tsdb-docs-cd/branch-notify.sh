#!/usr/bin/env bash

collect_current_branch_state() {
  local repo_url="$1"
  local assemble_config="$2"
  local output_file="$3"
  local output_dir
  local tmp_file

  output_dir=$(dirname "${output_file}")
  tmp_file=$(mktemp "${output_dir}/branch-state.XXXXXX")

  if jq -r '.assembleVersions[] | .branch' "${assemble_config}" | sort -u | while IFS= read -r branch; do
    sha=$(git ls-remote "${repo_url}" "refs/heads/${branch}" | awk '{print $1}')
    if [ -z "${sha}" ]; then
      echo "collect_current_branch_state: unable to resolve branch ${branch}" >&2
      return 1
    fi
    printf '%s\t%s\n' "${branch}" "${sha}"
  done > "${tmp_file}"; then
    mv "${tmp_file}" "${output_file}"
    return 0
  fi

  rm -f "${tmp_file}"
  return 1
}

classify_changed_targets() {
  local old_state_file="$1"
  local new_state_file="$2"
  local saw_latest=0
  local saw_next=0

  while IFS=$'\t' read -r branch new_sha; do
    old_sha=$(awk -F '\t' -v branch="${branch}" '$1 == branch {print $2}' "${old_state_file}" 2>/dev/null || true)
    if [ "${new_sha}" != "${old_sha}" ]; then
      if [ "${branch}" = "3.0" ]; then
        saw_next=1
      else
        saw_latest=1
      fi
    fi
  done < "${new_state_file}"

  [ "${saw_latest}" = "1" ] && printf 'latest\n'
  [ "${saw_next}" = "1" ] && printf 'next\n'
  return 0
}

build_success_notification() {
  local lang="$1"
  local job_url="$2"
  local url_prefix="$3"
  shift 3

  local heading='Click to visit:'
  if [ "${lang}" = "zh" ]; then
    heading='点击查看:'
  fi

  local out=$'✅ docs-cd deployed\njob: '"${job_url}"$'\n'"${heading}"
  local target
  for target in "$@"; do
    if [ "${target}" = "latest" ]; then
      out+=$'\nlatest: '"${url_prefix}"
    elif [ "${target}" = "next" ]; then
      out+=$'\nnext: '"${url_prefix%/}/next"
    else
      echo "build_success_notification: unknown target ${target}" >&2
      return 1
    fi
  done

  printf '%s' "${out}"
}
