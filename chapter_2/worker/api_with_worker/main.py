from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from datetime import datetime
import httpx
import sys

from bs4 import BeautifulSoup
from sqlalchemy.orm import Session, sessionmaker

import urllib.parse
import redis
import sqlalchemy.orm.session

from simplekiq import KiqQueue
from simplekiq import EventBuilder
from config import Config
from exceptions import UnicornException
from settings import Settings
from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator

import crud
import models
import database


app = FastAPI()
my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)
init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)


rconn = redis.StrictRedis(conf.section('sidekiq')['host'], int(conf.section('sidekiq')['port']))
queue = KiqQueue(rconn, "api_worker", True)
failed_queue = KiqQueue(rconn, "api_failed", True)
event_builder = EventBuilder(queue)

models.Base.metadata.create_all(bind=database.engine)


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
