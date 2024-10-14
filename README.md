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

This Python class is responsible for managing the process of downloading PDF files. It downloads the files from specified URLs and updates the `download_state.json` file to track progress. If any PDF fails to download successfully, it logs the details in an `error.json` file for further review.

### Key Features

- **Complete PDF Download Management**: Handles the entire process of downloading PDF files.
- **Error Handling**: Unsuccessful PDF downloads are recorded in the `error.json` file, allowing for easy troubleshooting and retrying of failed downloads.



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
## 5. MongoDbManger (`MongoDbManger.py`)

The `MongoDbManger.py` script is responsible for bulk inserting PDF data into MongoDB Cloud. It connects to your MongoDB database and efficiently uploads multiple PDF records in a single operation, improving performance and reducing the number of network calls.

Ensure that your MongoDB Cloud connection details (such as the URI, database name, and collection name) are correctly configured in the appropriate configuration file (`config.py` or environment variables) before running this script.

The `MongoDbManger.py` script is automatically run by the `script.sh` every 15 minutes as part of the process to upload PDFs to the cloud database.


## 6. Configuration (`config.py`)

The project provides a configuration file that allows you to set parameters to control the behavior of the PDF downloader.

## 7. Running the script

To execute the project and run the Python scripts periodically, it is recommended to use **Bash** to run the `script.sh` file. Bash ensures compatibility across different environments and helps manage the script's infinite loop and timed execution.

### Steps to run the script using Bash:

1. Ensure that your virtual environment is set up and all necessary dependencies are installed (the `venv` folder should exist in the project directory).
2. Open a terminal and navigate to the project directory.
3. Run the following command using Bash:

   ```bash
   ./script.sh
   ```
   
   


