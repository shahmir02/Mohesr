#!/usr/bin/env bash
# One-click execution script for Medallion DQ Pipeline & Allure Testing

set -e

# Find Python executable
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.venv/bin/python" ]; then
    PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
elif [ -f "/Users/mirs/PyCharmMiscProject/.venv/bin/python3" ]; then
    PYTHON_BIN="/Users/mirs/PyCharmMiscProject/.venv/bin/python3"
elif command -v python3 &>/dev/null; then
    PYTHON_BIN="python3"
else
    PYTHON_BIN="python"
fi

echo "================================================================================"
echo " Starting Medallion DQ Framework & Scorecard Generator"
echo " Using Python: $PYTHON_BIN"
echo "================================================================================"

$PYTHON_BIN run_all.py
