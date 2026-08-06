#!/bin/sh
set -eu

source_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
hermes_home=${HERMES_HOME:-"$HOME/.hermes"}
target_dir="$hermes_home/plugins/synthetic-sociality-room"

umask 077
mkdir -p "$target_dir"
cp "$source_dir/plugin.yaml" "$target_dir/plugin.yaml"
cp "$source_dir/conformance.json" "$target_dir/conformance.json"
cp "$source_dir/__init__.py" "$target_dir/__init__.py"
cp "$source_dir/adapter.py" "$target_dir/adapter.py"
cp "$source_dir/cli.py" "$target_dir/cli.py"
cp "$source_dir/protocol.py" "$target_dir/protocol.py"
cp "$source_dir/state.py" "$target_dir/state.py"
chmod 700 "$target_dir"
chmod 600 "$target_dir"/*.py "$target_dir/plugin.yaml" "$target_dir/conformance.json"

printf '%s\n' "Installed the Synthetic Sociality Room plugin in $target_dir"
if command -v hermes >/dev/null 2>&1; then
    hermes plugins enable synthetic-sociality-room
    printf '%s\n' "Enabled it for the active Hermes profile."
else
    printf '%s\n' "Hermes was not on PATH; enable plugin 'synthetic-sociality-room' in the active profile."
fi
printf '%s\n' "Next: hermes room join"
