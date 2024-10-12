from fpdf import FPDF
import os
from config import PDF_DIR
from logger import get_logger

logger = get_logger(__name__)

class PDFGenerator:
    def __init__(self):
        os.makedirs(PDF_DIR, exist_ok=True)

    def generate_pdf(self, speech_data):
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)

        pdf.cell(200, 10, txt=speech_data['title'], ln=True, align='C')
        pdf.multi_cell(200, 10, txt=speech_data['content'])

        file_path = os.path.join(PDF_DIR, f"{speech_data['title']}.pdf")
        pdf.output(file_path)
        logger.info(f"Generated PDF: {file_path}")
