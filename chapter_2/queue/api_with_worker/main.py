from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from datetime import datetime
import sys

from simplekiq import KiqQueue
from simplekiq import EventBuilder
from config import Config
from exceptions import UnicornException
from settings import Settings
from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator

import redis
import crud
import database


app = FastAPI()
my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)
init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)

templates = Jinja2Templates(directory="templates/")

queue = KiqQueue(conf.section('sidekiq')['host'], conf.section('sidekiq')['queue'], True)
failed_queue = KiqQueue(conf.section('sidekiq')['host'], conf.section('sidekiq')['failed_queue'], True)
event_builder = EventBuilder(queue)


database.init_database(conf.section('database')['url'])


def get_db():
    return database.Session()


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )


@app.get("/api/v1/url/")
async def scrap(url: str):
    try:
        value = event_builder.emit("scrap", {"url": url})
        queue.enqueue(value)
        return True
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))


@app.get("/api/v1/list")
async def list(request: Request):
    try:
        results = crud.list(get_db())
        return templates.TemplateResponse('demo.html', context={'request': request, 'results': results})
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))
