from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cprofile.profiler import CProfileMiddleware
from pydantic import BaseModel
from datetime import datetime

from prometheus_fastapi_instrumentator import Instrumentator, metrics
from bs4 import BeautifulSoup
from exceptions import UnicornException
from settings import Settings
from config import Config

import logging
import json_logging
import http3
import sys

from guid import Snowflake


app = FastAPI()

json_logging.init_fastapi(enable_json=True)
json_logging.init_request_instrument(app)
logger = json_logging.get_request_logger()
logger.addHandler(logging.handlers.TimedRotatingFileHandler("guid.log", when='h'))
json_logging.init_request_instrument(app)

settings = Settings()
config = Config(settings.CONFIG_PATH)
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

def init_snowflake(config):
    guid = config.section("guid")
    snowflake = Snowflake(int(guid['DATACENTER_ID']), int(guid['WORKER_ID']))
    return snowflake

snowflake = init_snowflake(config)

@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )


@app.get("/api/v1/guid/")
async def guid():
    try:
        return {"guid": snowflake.next()}
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))
