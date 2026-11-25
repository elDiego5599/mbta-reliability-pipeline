#!/usr/bin/env bash
# Unload the pipeline's launchd user agents and delete their plists. data/ is never touched.
set -euo pipefail

domain="gui/$(id -u)"
for agent in poller compact static prune; do
  label="io.github.eldiego5599.mbta.$agent"
  launchctl bootout "$domain/$label" 2>/dev/null || true
  rm -f "$HOME/Library/LaunchAgents/$label.plist"
  echo "removed $label"
done
