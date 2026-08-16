#!/bin/sh
set -eu

source_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
hermes_root=${HERMES_HOME:-"$HOME/.hermes"}
hermes_home=$hermes_root

# Hermes keeps named profiles below the default home and redirects its CLI to
# the sticky active profile before command dispatch. Mirror that resolution so
# the copied plugin and `hermes plugins enable` always address the same profile.
if [ -z "${HERMES_HOME:-}" ] && [ -f "$hermes_root/active_profile" ]; then
    active_profile=$(sed -n '1p' "$hermes_root/active_profile" | tr -d '\r\n')
    case "$active_profile" in
        ""|default) ;;
        *[!A-Za-z0-9._-]*|.|..) printf '%s\n' "Ignoring an invalid Hermes active profile name." >&2 ;;
        *)
            profile_home="$hermes_root/profiles/$active_profile"
            if [ -d "$profile_home" ]; then
                hermes_home=$profile_home
            else
                printf '%s\n' "Hermes active profile '$active_profile' was not found at $profile_home." >&2
                exit 1
            fi
            ;;
    esac
fi
target_dir="$hermes_home/plugins/synthetic-sociality-room"
plugins_dir="$hermes_home/plugins"
backups_dir="$hermes_home/backups"

# Hermes discovers every direct child manifest and keys plugins by manifest
# name. Refuse an ambiguous loader namespace before creating a staging or
# backup directory. Existing conflicts require an explicit, reversible
# operator cleanup outside the loader search path.
manifest_name() {
    manifest=$1
    [ ! -L "$manifest" ] || return 2
    [ -f "$manifest" ] || return 1
    name_lines=$(awk '/^[[:space:]]*name[[:space:]]*:/ { count++ } END { print count + 0 }' "$manifest")
    [ "$name_lines" -eq 1 ] || return 2
    raw_name=$(sed -n 's/^[[:space:]]*name[[:space:]]*:[[:space:]]*//p' "$manifest")
    raw_name=$(printf '%s' "$raw_name" | sed 's/[[:space:]]*$//')
    case "$raw_name" in
        \"*\")
            parsed_name=$(printf '%s' "$raw_name" | sed -n 's/^"\([A-Za-z0-9._\/-][A-Za-z0-9._\/-]*\)"$/\1/p')
            ;;
        \'*\')
            parsed_name=$(printf '%s' "$raw_name" | sed -n "s/^'\\([A-Za-z0-9._\/-][A-Za-z0-9._\/-]*\\)'$/\\1/p")
            ;;
        *)
            parsed_name=$(printf '%s' "$raw_name" | sed 's/[[:space:]]*#.*$//' | sed 's/[[:space:]]*$//')
            case "$parsed_name" in
                ""|*[!A-Za-z0-9._/-]*) parsed_name="" ;;
            esac
            ;;
    esac
    [ -n "$parsed_name" ] || return 2
    printf '%s\n' "$parsed_name"
}

if [ -L "$target_dir" ]; then
    printf '%s\n' "Refusing to replace symlinked plugin target: $target_dir" >&2
    exit 1
fi
if [ -d "$plugins_dir" ]; then
    # These three patterns cover every direct child, including dot-prefixed
    # names, without ever expanding the special . and .. entries.
    for candidate in "$plugins_dir"/* "$plugins_dir"/.[!.]* "$plugins_dir"/..?*; do
        [ -e "$candidate" ] || [ -L "$candidate" ] || continue
        [ "$candidate" != "$target_dir" ] || continue
        if [ -L "$candidate" ]; then
            printf '%s\n' "Refusing ambiguous plugin directory path: $candidate" >&2
            exit 1
        fi
        [ -d "$candidate" ] || continue
        yaml_manifest="$candidate/plugin.yaml"
        yml_manifest="$candidate/plugin.yml"
        if [ -L "$yaml_manifest" ] || [ -L "$yml_manifest" ]; then
            printf '%s\n' "Refusing ambiguous plugin manifest path in: $candidate" >&2
            exit 1
        fi
        if [ -f "$yaml_manifest" ]; then
            candidate_manifest=$yaml_manifest
        elif [ -f "$yml_manifest" ]; then
            candidate_manifest=$yml_manifest
        else
            continue
        fi
        if ! candidate_name=$(manifest_name "$candidate_manifest"); then
            printf '%s\n' "Refusing an ambiguous plugin manifest: $candidate_manifest" >&2
            exit 1
        fi
        if [ "$candidate_name" = "synthetic-sociality-room" ]; then
            printf '%s\n' "Refusing duplicate Hermes plugin name 'synthetic-sociality-room': $candidate_manifest" >&2
            printf '%s\n' "Move the explicit conflicting directory to a protected backup outside $plugins_dir, then retry." >&2
            exit 1
        fi
    done
fi

umask 077
mkdir -p "$plugins_dir" "$backups_dir"
stage_dir=$(mktemp -d "$plugins_dir/.synthetic-sociality-room.XXXXXX")
cleanup() {
    if [ -d "$stage_dir" ]; then
        rm -rf -- "$stage_dir"
    fi
}
trap cleanup EXIT HUP INT TERM
for file in README.md __init__.py adapter.py cli.py conformance.json context.py install.sh plugin.yaml protocol.py room_tools.py state.py; do
    cp "$source_dir/$file" "$stage_dir/$file"
done
chmod 700 "$stage_dir" "$stage_dir/install.sh"
chmod 600 "$stage_dir"/*.py "$stage_dir/README.md" "$stage_dir/plugin.yaml" "$stage_dir/conformance.json"

backup_dir=""
if [ -d "$target_dir" ]; then
    backup_dir="$backups_dir/synthetic-sociality-room-pre-install-$(date -u +%Y%m%dT%H%M%SZ)-$$"
    mv "$target_dir" "$backup_dir"
    printf '%s\n' "Preserved the previous plugin at $backup_dir"
fi
mv "$stage_dir" "$target_dir"
trap - EXIT HUP INT TERM

printf '%s\n' "Installed the Synthetic Sociality Room plugin in $target_dir"
if command -v hermes >/dev/null 2>&1; then
    if ! HERMES_HOME="$hermes_home" hermes plugins enable synthetic-sociality-room --no-allow-tool-override; then
        failed_dir="$backups_dir/synthetic-sociality-room-failed-install-$(date -u +%Y%m%dT%H%M%SZ)-$$"
        mv "$target_dir" "$failed_dir"
        if [ -n "$backup_dir" ] && [ -d "$backup_dir" ]; then
            mv "$backup_dir" "$target_dir"
        fi
        printf '%s\n' "Hermes rejected the plugin; restored the previous installation. Failed candidate: $failed_dir" >&2
        exit 1
    fi
    printf '%s\n' "Enabled it for the active Hermes profile."
else
    printf '%s\n' "Hermes was not on PATH; enable plugin 'synthetic-sociality-room' in the active profile."
fi
printf '%s\n' "Next: hermes room join"
