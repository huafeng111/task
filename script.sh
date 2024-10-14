#!/bin/bash

# Get the directory of the current script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set the virtual environment path using a relative path
VENV_PATH="$SCRIPT_DIR/venv/Scripts"

# Add the virtual environment's Python path to PATH
export PATH="$VENV_PATH:$PATH"

# Infinite loop, runs every 15 minutes
while true; do
    # Execute Python scripts
    python src/core/Speech/SpeechDownloader.py
    python src/core/Speech/PdfHanlder/PdfHanlder.py
    python src/core/Speech/PdfHanlder/UploadDb/MongoDbManger.py

    # Sleep for 15 minutes (15 minutes = 900 seconds)
    sleep 900
done
