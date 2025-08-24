#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if .venv folder exists
if [ ! -d "$SCRIPT_DIR/.venv" ]; then
    echo "Virtual environment not found. Creating new Python 3.9 virtual environment..."

    # Check if Python 3.9 is installed
    if command -v python3.9 &> /dev/null; then
        python3.9 -m venv "$SCRIPT_DIR/.venv"
        echo "Virtual environment created successfully."
    else
        echo "Error: Python 3.9 is not installed. Please install Python 3.9 and try again."
        exit 1
    fi
else
    echo "Virtual environment already exists."
fi

# Check if .installed file exists
if [ ! -f "$SCRIPT_DIR/.installed" ]; then
    echo "Installing packages from requirements.txt..."

    # Check if requirements.txt exists
    if [ ! -f "$SCRIPT_DIR/requirements.txt" ]; then
        echo "Error: requirements.txt not found in $SCRIPT_DIR"
        exit 1
    fi

    # Activate virtual environment and install packages
    source "$SCRIPT_DIR/.venv/bin/activate"

    if pip install -r "$SCRIPT_DIR/requirements.txt"; then
        # If installation was successful, create .installed file
        playwright install
        touch "$SCRIPT_DIR/.installed"
        echo "Packages installed successfully. Created .installed file."
    else
        echo "Error: Package installation failed."
        exit 1
    fi
else
    echo "Packages are already installed."
fi

echo "Setup completed successfully."