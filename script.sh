#!/bin/bash

# Add the virtual environment's Python path to PATH
export PATH="/c/Users/admin/Documents/GitHub/task/venv/Scripts:$PATH"

# Infinite loop, runs every 15 minutes
while true; do
    # Execute Python scripts
    python src/core/Speech/SpeechDownloader.py
    python src/core/Speech/PdfHandler/PdfHanlder.py
    python src/core/Speech/PdfHandler/UploadDb/MongoDbManger.py

    # Sleep for 15 minutes (15 minutes = 900 seconds)
    sleep 900
done
