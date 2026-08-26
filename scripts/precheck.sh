#!/usr/bin/env bash
# The fast half of CI, run locally before pushing.
#
# Exists because a push failed CI on a shellcheck error that takes three seconds
# to catch here. This runs the two checks that need no Docker stack -- shellcheck
# and the Maven build with tests -- which is where every CI failure on this
# branch has come from so far. The two stack jobs (verify, cold start) still run
# on CI only; they need several minutes and a clean machine.
#
#   scripts/precheck.sh            both checks
#   scripts/precheck.sh --quick    shellcheck and compile only, no tests
set -euo pipefail
cd "$(dirname "$0")/.."

quick=0
[ "${1:-}" = "--quick" ] && quick=1

fail=0
step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

step "ShellCheck"
# Match CI exactly, including which files it looks at.
if command -v shellcheck >/dev/null 2>&1; then
  shellcheck scripts/*.sh docker/scripts/*.sh && echo "clean" || fail=1
elif command -v docker >/dev/null 2>&1; then
  echo "(no local shellcheck; using the container)"
  docker run --rm -v "$PWD:/mnt" -w /mnt koalaman/shellcheck:stable \
    scripts/*.sh docker/scripts/*.sh && echo "clean" || fail=1
else
  echo "SKIPPED: neither shellcheck nor docker is available" >&2
  fail=1
fi

if [ "$quick" -eq 1 ]; then
  step "Maven (compile only)"
  mvn -B -q --no-transfer-progress test-compile || fail=1
else
  step "Maven (build, unit and integration tests)"
  mvn -B --no-transfer-progress verify || fail=1
fi

echo
if [ "$fail" -ne 0 ]; then
  echo "precheck FAILED -- fix this before pushing" >&2
  exit 1
fi
echo "precheck passed. The stack jobs (verify, cold start) still run on CI."
