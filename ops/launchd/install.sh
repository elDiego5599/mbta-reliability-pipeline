#!/usr/bin/env bash
# Install the pipeline's launchd user agents from the templates next to this script.
# Usage: install.sh                   render, lint and (re)load the agents
#        install.sh --render-only DIR  only render the plists into DIR (used by the tests)
set -euo pipefail

AGENTS="poller compact static"
PREFIX="io.github.eldiego5599.mbta"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="$(cd "$script_dir/../.." && pwd -P)"

case "$*" in
  "") mode=install ;;
  "--render-only "*) [[ $# -eq 2 ]] || { echo "usage: $0 [--render-only DIR]" >&2; exit 2; }
    mode=render ;;
  *) echo "usage: $0 [--render-only DIR]" >&2; exit 2 ;;
esac

case "$ROOT" in
  *'&'* | *'<'* | *'>'*)
    echo "error: the repository path contains &, < or >, which would break the plist XML: $ROOT" >&2
    exit 1 ;;
esac

render() {  # render DIR: write one <label>.plist per agent into DIR
  mkdir -p "$1"
  for agent in $AGENTS; do
    content="$(cat "$script_dir/$agent.plist.template")"
    printf '%s\n' "${content//@REPO@/$ROOT}" > "$1/$PREFIX.$agent.plist"
  done
}

if [[ $mode == render ]]; then
  render "$2"
  exit 0
fi

[[ "$(uname -s)" == Darwin ]] || { echo "error: launchd agents need macOS" >&2; exit 1; }
for agent in $AGENTS; do
  [[ -x "$ROOT/.venv/bin/mbta-$agent" ]] || {
    echo "error: $ROOT/.venv/bin/mbta-$agent is missing; run uv sync first" >&2; exit 1; }
done

branch="$(git -C "$ROOT" branch --show-current 2>/dev/null || true)"
if [[ "$branch" != main ]]; then
  echo "warning: $ROOT is on '${branch:-no branch}', not main. The agents run whatever this" >&2
  echo "warning: checkout has checked out; keep it on main and do branch work in worktrees." >&2
fi

mkdir -p "$ROOT/data/logs"
target="$HOME/Library/LaunchAgents"
render "$target"
domain="gui/$(id -u)"
for agent in $AGENTS; do
  label="$PREFIX.$agent"
  plutil -lint -s "$target/$label.plist"
  if launchctl bootout "$domain/$label" 2>/dev/null; then
    # bootout returns before the job is gone; bootstrapping too early fails with error 5.
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      launchctl print "$domain/$label" >/dev/null 2>&1 || break
      sleep 0.5
    done
  fi
  launchctl bootstrap "$domain" "$target/$label.plist"
  echo "$label: $(launchctl print "$domain/$label" | grep -m1 'state =' | xargs)"
done
