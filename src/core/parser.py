import PyPDF2
from bs4 import BeautifulSoup
from utils.logger import get_logger

logger = get_logger(__name__)

def extract_pdf_content(file_path):
    try:
        with open(file_path, 'rb') as pdf_file:
            reader = PyPDF2.PdfReader(pdf_file)
            content = ""
            for page in reader.pages:
                content += page.extract_text()
            return content
    except Exception as e:
        logger.error(f"Error extracting content from {file_path}: {str(e)}")
        return None

def extract_html_content(html):
    try:
        soup = BeautifulSoup(html, "html.parser")
        paragraphs = soup.find_all("p")
        return "\n".join([p.get_text() for p in paragraphs])
    except Exception as e:
        logger.error(f"Error parsing HTML content: {str(e)}")
        return None
