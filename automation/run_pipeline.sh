#!/bin/bash
set -e

AUTOMATION_DIR="/home/michael/repo/automation"
PYTHON_SCRIPT="$AUTOMATION_DIR/sphere_batch_processor.py"

cd "$AUTOMATION_DIR"

echo "=========================================="
echo "SPHERE IFS Batch Automation Pipeline"
echo "=========================================="
echo "Raw data directory: $AUTOMATION_DIR/raw_data"
echo "Output directory: $AUTOMATION_DIR/reduced_data"
echo "Logs: $AUTOMATION_DIR/logs"
echo ""

if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "ERROR: Python script not found at $PYTHON_SCRIPT"
    exit 1
fi

python3 "$PYTHON_SCRIPT"
exit $?
