#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Check if .venv directory exists
if [ ! -d "$SCRIPT_DIR/.venv" ]; then
    echo "Error: Python virtual environment not found at $SCRIPT_DIR/.venv"
    echo "Please make sure the virtual environment is created properly."
    exit 1
fi

# Check if Main.py exists
if [ ! -f "$SCRIPT_DIR/Main.py" ]; then
    echo "Error: Main.py not found in $SCRIPT_DIR"
    exit 1
fi

# Activate the virtual environment and run the Python script
echo "Running Main.py with the Python virtual environment..."
source "$SCRIPT_DIR/.venv/bin/activate" && python "$SCRIPT_DIR/Main.py"

# Capture the exit code of the Python script
EXIT_CODE=$?

# Deactivate the virtual environment
deactivate

# Exit with the same code as the Python script
echo "Python script finished with exit code: $EXIT_CODE"
exit $EXIT_CODE