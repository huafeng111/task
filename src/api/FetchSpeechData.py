from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from io import BytesIO
from pypdf import PdfWriter
import motor.motor_asyncio
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
import os
import importlib.util

# Dynamically import config.py
def dynamic_import(module_name, module_path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Import configuration from config.py
current_script_dir = os.path.dirname(os.path.abspath(__file__))
config_module_path = os.path.abspath(
    os.path.join(current_script_dir, '..', '..', 'src', 'config', 'config.py')
)
config = dynamic_import("config", config_module_path)

# Initialize FastAPI app
app = FastAPI()

# MongoDB connection manager
class AsyncMongoDBManager:
    def __init__(self):
        try:
            mongo_uri = getattr(config, "MONGO_URI")
            database_name = getattr(config, "DATABASE_NAME")
            collection_name = getattr(config, "COLLECTION_NAME")
            self.client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
            self.db = self.client[database_name]
            self.collection = self.db[collection_name]
        except Exception as e:
            print(f"Failed to connect to MongoDB: {str(e)}")
            self.client = None

    async def find_document_by_title(self, title):
        if not self.client:
            return None
        return await self.collection.find_one({"title": title})

db_manager = AsyncMongoDBManager()

speech_app = FastAPI()
@speech_app.get("/get_pdf_by_title/")
async def get_pdf_by_title(title: str):
    print("done")
    title = title.strip()

    # Retrieve document from MongoDB
    document = await db_manager.find_document_by_title(title)

    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    # Get pages data
    pages = document.get("pages", [])
    if not pages:
        raise HTTPException(status_code=404, detail="No pages found in the document")

    # Create PDF in-memory using BytesIO
    buffer = BytesIO()

    # Use ReportLab to generate PDF with text content
    pdf_canvas = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    for page_text in pages:
        pdf_canvas.setFont("Helvetica", 12)
        # Splitting the text into lines to fit on the page (simple word wrapping)
        lines = page_text.split('\n')
        y_position = height - 40  # Starting point for the text

        for line in lines:
            if y_position < 40:  # If the text reaches the bottom of the page
                pdf_canvas.showPage()  # Create a new page
                y_position = height - 40  # Reset the position

            pdf_canvas.drawString(40, y_position, line)  # Draw each line
            y_position -= 14  # Move down to next line

        pdf_canvas.showPage()  # Add a new page for each entry

    pdf_canvas.save()  # Finalize the PDF content
    buffer.seek(0)

    # Return PDF file as a streaming response
    return StreamingResponse(buffer, media_type="application/pdf", headers={"Content-Disposition": f"attachment; filename={title}.pdf"})
