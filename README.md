# Configuration and Notes

This project is designed to download PDF files using a web scraper. Below are the key configurations and important notes for using the project.


## 1. `SpeechDownloader.py`

This Python class is responsible for managing the process of downloading PDF files. It downloads the files from specified URLs and updates the `download_state.json` file to track progress. 


### Key Features

- **Complete PDF Download Management**: Handles the entire process of downloading PDF files.



## 2. `download_state.json`

- This file records the pointer from the last scraping session, allowing the scraper to continue from where it left off.
- It tracks the last successfully processed year and ensures that duplicate downloads are avoided.

**Example content:**

```json
{
  "last_year": 2024
}


## 3. Configuration (`config.py`)

The project provides a configuration file that allows you to set parameters to control the behavior of the PDF downloader.








