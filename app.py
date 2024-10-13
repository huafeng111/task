from fastapi import FastAPI
from src.api.FetchSpeechData import speech_app
# ³õÊ¼»¯ FastAPI ÊµÀý
app = FastAPI()
app.mount("/s",speech_app)

@app.get("/")
async def root():
    return {"status": "ok"}


