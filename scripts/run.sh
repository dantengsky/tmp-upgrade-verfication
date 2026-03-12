#!/usr/bin/env bash
# Thin wrapper — delegates to run.py.
# Kept for backwards compatibility with CI scripts that invoke ./run.sh.
exec python3 "$(dirname "$0")/run.py" "$@"
