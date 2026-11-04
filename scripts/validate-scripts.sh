#!/bin/sh
# Shared offline validation used by platform ci verify.
# Run from the repository root.
set -eu

shellcheck scripts/platform scripts/platform.d/*.sh scripts/*.sh
sh scripts/check-scripts.sh
sh scripts/check-versions.sh
python3 scripts/test_ci_docs.py
sh scripts/test-check.sh
sh scripts/test-verify.sh
sh scripts/test-worktree-setup.sh
sh scripts/test-container-entrypoint.sh
python3 scripts/test_run_context.py
