from fastapi import FastAPI
from src.api.FetchSpeechData import speech_app
# ��ʼ�� FastAPI ʵ��
app = FastAPI()
app.mount("/s",speech_app)

@app.get("/")
async def root():
    return {"status": "ok"}


