#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [--render-only DIR]" >&2
}

if [[ $# -eq 1 && "$1" != "--render-only" ]]; then
  usage
  exit 2
fi

if [[ $# -eq 2 && "$1" == "--render-only" ]]; then
  render_only=1
  render_dir="$2"
elif [[ $# -eq 0 ]]; then
  render_only=0
else
  usage
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="$(cd "$script_dir/../.." && pwd -P)"

case "$ROOT" in
  *'&'*|*'<'*|*'>'*)
    echo "Error: the repository path contains &, <, or > and cannot be rendered safely in XML: $ROOT" >&2
    exit 1
    ;;
esac

render_plist() {
  template="$1"
  target="$2"
  label="$3"
  content="$(cat "$template")"
  content="${content//@REPO@/$ROOT}"
  printf '%s\n' "$content" > "$target/$label.plist"
}

if [[ "$render_only" -eq 1 ]]; then
  mkdir -p "$render_dir"
  render_plist "$script_dir/poller.plist.template" "$render_dir" "io.github.eldiego5599.mbta.poller"
  render_plist "$script_dir/compact.plist.template" "$render_dir" "io.github.eldiego5599.mbta.compact"
  render_plist "$script_dir/static.plist.template" "$render_dir" "io.github.eldiego5599.mbta.static"
  exit 0
fi

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "Error: launchd installation requires macOS (Darwin)." >&2
  exit 1
fi

for executable in mbta-poller mbta-compact mbta-static; do
  if [[ ! -x "$ROOT/.venv/bin/$executable" ]]; then
    echo "Error: missing $ROOT/.venv/bin/$executable; run uv sync." >&2
    exit 1
  fi
done

mkdir -p "$ROOT/data/logs"

target="$HOME/Library/LaunchAgents"
mkdir -p "$target"

render_plist "$script_dir/poller.plist.template" "$target" "io.github.eldiego5599.mbta.poller"
render_plist "$script_dir/compact.plist.template" "$target" "io.github.eldiego5599.mbta.compact"
render_plist "$script_dir/static.plist.template" "$target" "io.github.eldiego5599.mbta.static"

for label in \
  io.github.eldiego5599.mbta.poller \
  io.github.eldiego5599.mbta.compact \
  io.github.eldiego5599.mbta.static
do
  plutil -lint "$target/$label.plist"
done

branch="$(git -C "$ROOT" branch --show-current 2>/dev/null || true)"
if [[ "$branch" != "main" ]]; then
  echo "Warning: this checkout is on '${branch:-an unknown branch}', but the agents run whatever this checkout has checked out." >&2
  echo "Warning: the checkout used by the agents should stay on main." >&2
fi

uid="$(id -u)"
domain="gui/$uid"
for label in \
  io.github.eldiego5599.mbta.poller \
  io.github.eldiego5599.mbta.compact \
  io.github.eldiego5599.mbta.static
do
  launchctl bootout "$domain/$label" 2>/dev/null || true
  launchctl bootstrap "$domain" "$target/$label.plist"
done

for label in \
  io.github.eldiego5599.mbta.poller \
  io.github.eldiego5599.mbta.compact \
  io.github.eldiego5599.mbta.static
do
  output="$(launchctl print "$domain/$label" 2>/dev/null || true)"
  echo "$label"
  echo "$output" | grep "state =" | head -n 1 || true
done
