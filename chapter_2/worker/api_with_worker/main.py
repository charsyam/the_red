from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cprofile.profiler import CProfileMiddleware
from pydantic import BaseModel

from exceptions import UnicornException

from settings import get_settings

import logging
import json_logging
from datetime import datetime
import http3
import sys

from prometheus_fastapi_instrumentator import Instrumentator, metrics
from bs4 import BeautifulSoup

import urllib.parse
import redis
from simplekiq import KiqQueue
from simplekiq import EventBuilder
from config import Config

from sqlalchemy.orm import Session, sessionmaker

import sqlalchemy.orm.session
import crud
import models
import database


app = FastAPI()

json_logging.init_fastapi(enable_json=True)
json_logging.init_request_instrument(app)
logger = json_logging.get_request_logger()
logger.addHandler(logging.handlers.TimedRotatingFileHandler("scrap.log", when='h'))
json_logging.init_request_instrument(app)


config = Config(get_settings().CONFIG_PATH)

Instrumentator().instrument(app).expose(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

instrumentator = Instrumentator(
    should_group_status_codes=False,
    should_ignore_untemplated=True,
    should_respect_env_var=True,
    should_instrument_requests_inprogress=True,
    excluded_handlers=[".*admin.*", "/metrics"],
    env_var_name="ENABLE_METRICS",
    inprogress_name="inprogress",
    inprogress_labels=True,
)

instrumentator.add(
    metrics.request_size(
        should_include_handler=True,
        should_include_method=False,
        should_include_status=True,
        metric_namespace="a",
        metric_subsystem="b",
    )
).add(
    metrics.response_size(
        should_include_handler=True,
        should_include_method=False,
        should_include_status=True,
        metric_namespace="namespace",
        metric_subsystem="subsystem",
    )
)


client = http3.AsyncClient()

rconn = redis.StrictRedis(config.section('sidekiq')['host'], int(config.section('sidekiq')['port']))
queue = KiqQueue(rconn, "api_worker", True)
failed_queue = KiqQueue(rconn, "api_failed", True)
event_builder = EventBuilder(queue)

models.Base.metadata.create_all(bind=database.engine)


def get_db():
    db = database.SessionLocal()
    return db


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
