
## Main Files Description

### 1. `download_state.json`

This file is used to keep track of the scraping progress and state. Every time the scraper is run, the current download progress and pointer information are recorded in this file. The next time the scraper is executed, it will resume from where it left off, preventing duplicate downloads.

**Sample content:**

```json
{
  "last_downloaded": "https://example.com/speech.pdf",
  "downloaded_files": [
    "speech1.pdf",
    "speech2.pdf"
  ]
}
