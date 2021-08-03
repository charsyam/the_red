from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime

from bs4 import BeautifulSoup
from exceptions import UnicornException
from settings import Settings
from config import Config

import logging
import json_logging
import httpx
import sys

from guid import Snowflake
from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator


app = FastAPI()

my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)
init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)

client = httpx.AsyncClient()


def init_snowflake(conf):
    guid = conf.section("guid")
    snowflake = Snowflake(int(guid['DATACENTER_ID']), int(guid['WORKER_ID']))
    return snowflake

snowflake = init_snowflake(conf)

@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )


@app.get("/api/v1/guid/")
async def guid():
    try:
        n = snowflake.next()
        return {"guid": n, "guid_str": str(n)}
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))
