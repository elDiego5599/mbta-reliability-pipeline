#!/usr/bin/env bash
set -euo pipefail

target="$HOME/Library/LaunchAgents"
uid="$(id -u)"
domain="gui/$uid"

for label in \
  io.github.eldiego5599.mbta.poller \
  io.github.eldiego5599.mbta.compact \
  io.github.eldiego5599.mbta.static
do
  launchctl bootout "$domain/$label" 2>/dev/null || true
  plist="$target/$label.plist"
  rm -f "$plist"
  echo "Removed $plist"
done
