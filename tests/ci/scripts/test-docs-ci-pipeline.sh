#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)

grep -F "check-with-typos:" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "check-with-autocorrect:" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "check-with-markdownlint:" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "build-doc:" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "workflow:" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F '$CI_PIPELINE_SOURCE == "parent_pipeline"' "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "extends: .docs-child-job" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "tags: [X64, Linux, TSDB-DOCS, u0-30]" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F ".gitlab/scripts/tsdb-docs-ci/local-validate.sh --fix" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "bash .gitlab/scripts/tsdb-docs-ci/run-in-docker.sh bash .gitlab/scripts/tsdb-docs-ci/check-typos.sh" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "bash .gitlab/scripts/tsdb-docs-ci/run-in-docker.sh bash .gitlab/scripts/tsdb-docs-ci/check-autocorrect.sh" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "bash .gitlab/scripts/tsdb-docs-ci/run-in-docker.sh bash .gitlab/scripts/tsdb-docs-ci/check-markdownlint.sh" "${ROOT}/.gitlab/tsdb-build-docs.yml"
if grep -F "docs-autofix:" "${ROOT}/.gitlab/tsdb-build-docs.yml"; then
  echo "docs-autofix must not run in CI; use .gitlab/scripts/tsdb-docs-ci/local-validate.sh --fix locally" >&2
  exit 1
fi
if grep -F "docs-autofix.patch" "${ROOT}/.gitlab/tsdb-build-docs.yml"; then
  echo "CI must not publish autofix patch artifacts" >&2
  exit 1
fi
grep -F "bash .gitlab/scripts/tsdb-docs-ci/run-in-docker.sh bash .gitlab/scripts/tsdb-docs-ci/build-doc.sh" "${ROOT}/.gitlab/tsdb-build-docs.yml"
grep -F "needs:" "${ROOT}/.gitlab/tsdb-build-docs.yml"

# Parent pipeline assertions (repo-root .gitlab-ci.yml)
grep -F "stages:" "${ROOT}/.gitlab-ci.yml"
grep -F "trigger:" "${ROOT}/.gitlab-ci.yml"
grep -F ".gitlab/tsdb-build-docs.yml" "${ROOT}/.gitlab-ci.yml"
grep -F "strategy: depend" "${ROOT}/.gitlab-ci.yml"
grep -F "source/taos-community/docs/**" "${ROOT}/.gitlab-ci.yml"
grep -F '$CI_MERGE_REQUEST_TARGET_BRANCH_NAME == "main"' "${ROOT}/.gitlab-ci.yml"
grep -F '$CI_MERGE_REQUEST_TARGET_BRANCH_NAME == "3.0"' "${ROOT}/.gitlab-ci.yml"
grep -F '$CI_MERGE_REQUEST_TARGET_BRANCH_NAME =~ /^3\.3\.6/' "${ROOT}/.gitlab-ci.yml"
grep -F '$CI_MERGE_REQUEST_TARGET_BRANCH_NAME == "docs-cloud"' "${ROOT}/.gitlab-ci.yml"

ruby -e '
  require "yaml"
  ci = YAML.load_file(ARGV.fetch(0))
  rules = ci.fetch(".rules-code-change")
  if rules.key?("extends")
    warn ".rules-code-change must not extend itself or any job"
    exit 1
  end
  %w[prepare-workspace build-externals check-assert unit-test upload-nexus coordinator].each do |job|
    unless ci.key?(job)
      warn "#{job} must be a top-level job"
      exit 1
    end
  end
' "${ROOT}/.gitlab/.gitlab-ci.yml"
