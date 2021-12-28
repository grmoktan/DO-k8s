from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from kafka_clients.consumer import consume
from kafka_clients.producer import produce
from typing import Optional

app = FastAPI()
templates = Jinja2Templates(directory="templates/")


@app.get("/")
def form_post(request: Request):
    result = ""
    return templates.TemplateResponse('form.html', context={'request': request, 'result': result})

@app.post("/")
def form_post(request: Request, message_for_q: Optional[str] = Form(None)):
    res = produce(message_for_q)
    result = consume()
    return templates.TemplateResponse('form.html', context={'request': request, 'result': result })


