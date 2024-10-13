import os
import json
import pandas as pd

def load_csv_data(csv_file_path):
    """Load the CSV file and return a list of all PDF file paths."""
    if not os.path.exists(csv_file_path):
        print(f"CSV file not found: {csv_file_path}")
        return None

    # Load the CSV file into a pandas DataFrame
    csv_data = pd.read_csv(csv_file_path)

    # Extract all file paths from the 'file_path' column
    pdf_paths_in_csv = csv_data['file_path'].tolist()

    return pdf_paths_in_csv

def load_json_data(json_file_path):
    """Load the JSON file and return a list of all processed PDF file paths."""
    if not os.path.exists(json_file_path):
        print(f"JSON file not found: {json_file_path}")
        return None

    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

    # Extract the file paths from the csv_metadata for each entry in the JSON
    pdf_paths_in_json = [entry['csv_metadata']['file_path'] for entry in json_data]

    return pdf_paths_in_json

def validate_pdfs(csv_file_path, json_file_path):
    """Compare CSV file paths with JSON file paths to ensure all PDFs are processed."""
    csv_pdf_paths = load_csv_data(csv_file_path)
    json_pdf_paths = load_json_data(json_file_path)

    if csv_pdf_paths is None or json_pdf_paths is None:
        print("Validation failed due to missing files.")
        return

    # Find PDFs in CSV that are missing in the JSON file
    missing_pdfs = [pdf for pdf in csv_pdf_paths if pdf not in json_pdf_paths]

    if not missing_pdfs:
        print("Validation successful: All PDFs in the CSV are present in the JSON.")
    else:
        print(f"Validation failed: The following PDFs are missing in the JSON:\n{missing_pdfs}")

if __name__ == "__main__":
    csv_relative_path = "../../../../data/pdfs/speech_metadata.csv"  # CSV file path
    json_file_path = "../all_metadata_and_text.json"  # Generated JSON file path

    # Perform the validation
    validate_pdfs(csv_relative_path, json_file_path)
