# How to Run the Project

### 1. Install Required Dependencies

Ensure you have Python 3.12 installed. Then, install the required dependencies by running the following command:

```bash
pip install -r requirements.txt
```

### 2. Start the Project

Once the dependencies are installed, you can start the project using `uvicorn`. Use the following command:

```bash
uvicorn app:app --host 0.0.0.0 --port 5000 --reload
```

# Configuration and Notes

This project is designed to download PDF files using a web scraper. Below are the key configurations and important notes for using the project.


## 1. `SpeechDownloader.py`

This Python class is responsible for managing the process of downloading PDF files. It downloads the files from specified URLs and updates the `download_state.json` file to track progress. 


### Key Features

- **Complete PDF Download Management**: Handles the entire process of downloading PDF files.


## 2. `pdfHandler.py`

The `pdfHandler` module is responsible for converting the content of the downloaded PDF files into structured metadata and storing it for further use.

### Key Features

- **PDF Content to Metadata Conversion**: Extracts information from the PDF files and stores it as metadata.

## 3. `src/api/FetchSpeechData.py`

The `get_pdf_by_title` function in `FetchSpeechData.py` allows you to retrieve and parse the PDF metadata from MongoDB based on the PDF title, converting it back into a downloadable PDF.

```bash
curl --location 'http://localhost:5000/s/get_pdf_by_title/?title=The%20Economic%20Outlook%20and%20Monetary%20Policy' \
--header 'Content-Type: application/json' \
--data ''
```
## 4. `download_state.json`

- This file records the pointer from the last scraping session, allowing the scraper to continue from where it left off.
- It tracks the last successfully processed year and ensures that duplicate downloads are avoided.

**Example content:**

```json
{
  "last_year": 2024
}

```
## 5. Configuration (`config.py`)

The project provides a configuration file that allows you to set parameters to control the behavior of the PDF downloader.



## 6. Running the script

To run the project and execute the Python scripts periodically, you can use the `script.sh` file. This script is set up to run the necessary Python scripts every 15 minutes.

### Steps to run the script:

1. Ensure that your virtual environment is created and installed with all the necessary dependencies (`venv` folder should be in the project directory).
2. Open a terminal and navigate to the project directory.
3. Run the following command to start the script:

   ```bash
   ./script.sh


