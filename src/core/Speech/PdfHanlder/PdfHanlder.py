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

    def save_all_metadata(self, all_metadata, output_file):
        # Save all extracted metadata and text to a single JSON file
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(all_metadata, f, ensure_ascii=False, indent=4)
            print(f"All metadata and text saved to {output_file}")
        except Exception as e:
            print(f"Failed to save all metadata: {e}")

    def process_all_pdfs(self, output_file):
        # Process all PDFs listed in the CSV file and save to one JSON file
        all_metadata = []  # List to store metadata for all PDFs

        for index, row in self.csv_data.iterrows():
            relative_pdf_path = row['file_path']
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

        # Save all metadata to a single JSON file
        self.save_all_metadata(all_metadata, output_file)


if __name__ == "__main__":
    csv_relative_path = "../../../data/pdfs/speech_metadata.csv"  # Adjust the relative path based on your directory structure
    output_metadata_file = "all_metadata_and_text.json"  # Output JSON file to store all metadata

    handler = PDFHandler(csv_relative_path)

    # Process all PDFs listed in the CSV and save their metadata into one JSON file
    handler.process_all_pdfs(output_metadata_file)
