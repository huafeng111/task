import os
import fitz  # PyMuPDF
import json
import pandas as pd

class PDFHandler:
    def __init__(self, csv_relative_path):
        # Get the current script directory and construct the relative path
        script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the absolute path of the current script
        self.csv_file = os.path.abspath(os.path.join(script_dir, csv_relative_path))  # Find CSV file using relative path
        self.base_dir = os.path.dirname(self.csv_file)  # Base directory for the CSV file

        # Print the paths to check if they are correct
        print("CSV file path:", self.csv_file)
        print("Base directory:", self.base_dir)
        print("Current working directory:", os.getcwd())

        if not os.path.exists(self.csv_file):
            print(f"File not found: {self.csv_file}")
            return

        # Load the CSV data into a pandas DataFrame
        self.csv_data = pd.read_csv(self.csv_file)
        print("CSV data loaded successfully")

    def extract_pdf_metadata(self, file_path):
        # Extract metadata and text from a PDF file using PyMuPDF (fitz)
        try:
            # Change the working directory to the base directory to handle relative paths
            original_cwd = os.getcwd()
            os.chdir(self.base_dir)  # Change to the base directory where PDFs are located

            doc = fitz.open(file_path)  # Open the PDF file using the relative path
            metadata = doc.metadata  # Get metadata of the PDF
            pages = []
            for page_num in range(doc.page_count):
                page = doc.load_page(page_num)  # Load a single page
                text = page.get_text("text")  # Extract text from the page
                pages.append(text)  # Append text for each page

            os.chdir(original_cwd)  # Switch back to the original directory

            return {"metadata": metadata, "pages": pages}
        except Exception as e:
            print(f"Could not read PDF file {file_path}: {e}")
            os.chdir(original_cwd)  # Ensure we switch back even if there was an error
            return None

    def get_csv_metadata(self, relative_pdf_path):
        # Find the metadata for the PDF file from the CSV
        try:
            csv_row = self.csv_data[self.csv_data['file_path'] == relative_pdf_path]
            if not csv_row.empty:
                # Convert the row to a dictionary for easier use
                return csv_row.iloc[0].to_dict()
            else:
                print(f"No matching entry found in CSV for file: {relative_pdf_path}")
                return None
        except Exception as e:
            print(f"Error fetching CSV metadata: {e}")
            return None

    def load_existing_metadata(self, output_file):
        # Load existing metadata if the file exists
        if os.path.exists(output_file):
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    all_metadata = json.load(f)
                print(f"Existing metadata loaded from {output_file}")
                return all_metadata
            except Exception as e:
                print(f"Error loading existing metadata: {e}")
                return []
        else:
            return []

    def save_all_metadata(self, all_metadata, output_file):
        # Save all extracted metadata and text to a single JSON file
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(all_metadata, f, ensure_ascii=False, indent=4)
            print(f"All metadata and text saved to {output_file}")
        except Exception as e:
            print(f"Failed to save all metadata: {e}")

    def process_all_pdfs(self, output_file):
        # Load existing metadata (if available)
        all_metadata = self.load_existing_metadata(output_file)
        existing_pdf_paths = {entry['csv_metadata']['file_path'] for entry in all_metadata}

        # Process all PDFs listed in the CSV file
        for index, row in self.csv_data.iterrows():
            relative_pdf_path = row['file_path']

            # Check if the PDF is already processed and in the metadata
            if relative_pdf_path in existing_pdf_paths:
                print(f"Skipping already processed PDF: {relative_pdf_path}")
                continue

            # Extract metadata from the PDF
            pdf_data = self.extract_pdf_metadata(relative_pdf_path)

            if pdf_data:
                # Get additional metadata from CSV
                csv_metadata = row.to_dict()  # Directly use the row as CSV metadata

                # Combine both PDF metadata and CSV metadata
                combined_metadata = {
                    "pdf_metadata": pdf_data['metadata'],
                    "pages": pdf_data['pages'],
                    "csv_metadata": csv_metadata
                }

                # Add the combined metadata to the list
                all_metadata.append(combined_metadata)

        # Save all metadata (newly processed + previously existing) to a single JSON file
        self.save_all_metadata(all_metadata, output_file)

    def validate_pdfs_in_json(self, json_file_path):
        """Compare CSV file paths with JSON file paths to ensure all PDFs are processed."""
        # Extract all PDF file paths from the CSV
        csv_pdf_paths = self.csv_data['file_path'].tolist()

        # Load JSON data
        if not os.path.exists(json_file_path):
            print(f"JSON file not found: {json_file_path}")
            return

        with open(json_file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        # Extract the file paths from the csv_metadata for each entry in the JSON
        json_pdf_paths = [entry['csv_metadata']['file_path'] for entry in json_data]

        # Find PDFs in CSV that are missing in the JSON file
        missing_pdfs = [pdf for pdf in csv_pdf_paths if pdf not in json_pdf_paths]

        if not missing_pdfs:
            print("Validation successful: All PDFs in the CSV are present in the JSON.")
        else:
            print(f"Validation failed: The following PDFs are missing in the JSON:\n{missing_pdfs}")

if __name__ == "__main__":
    csv_relative_path = "../../../data/pdfs/speech_metadata.csv"  # Adjust the relative path based on your directory structure
    output_metadata_file = "all_metadata_and_text.json"  # Output JSON file to store all metadata

    handler = PDFHandler(csv_relative_path)

    # Process all PDFs listed in the CSV and save their metadata into one JSON file
    handler.process_all_pdfs(output_metadata_file)

    # Validate that all PDFs in the CSV are present in the generated JSON
    handler.validate_pdfs_in_json(output_metadata_file)
