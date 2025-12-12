#!/bin/bash
MONITORED_DIR="/home/michael/automation/raw_data"
PYTHON_SCRIPT="/home/michael/automation/automate.py"

inotifywait -m -e close_write "$MONITORED_DIR" | while read DIR EVENT FILE
do
    echo "Detected event: $EVENT on $DIR$FILE"

    # Ignore Windows Zone.Identifier streams
    if [[ "$FILE" == *":Zone.Identifier" ]]; then
        echo "Ignoring Zone.Identifier stream for $DIR$FILE"
        continue
    fi

    echo "Starting Python on: $DIR$FILE"
    python "$PYTHON_SCRIPT" "$DIR$FILE"
    echo "Python finished with exit code $?"
done
