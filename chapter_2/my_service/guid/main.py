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
import sys

from guid import Snowflake
from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from zoo import init_kazoo


app = FastAPI()

my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)
init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)


zk = init_kazoo(conf.section("zookeeper")["hosts"], None, None)
ZK_PATH = conf.section("zookeeper")["path"]


def register_into_service_discovery(endpoint):
    node_path = f"{ZK_PATH}/{endpoint}"
    if zk.exists(node_path):
        zk.delete(node_path)
    zk.create(node_path, ephemeral=True, makepath=True)

@app.on_event("startup")
def startup():
    register_into_service_discovery(my_settings.APP_ENDPOINT)


def init_snowflake(conf):
    snowflake = Snowflake(my_settings.DATACENTER_ID, my_settings.WORKER_ID)
    print(f"DATACENTER_ID: {my_settings.DATACENTER_ID}, WORKER_ID: {my_settings.WORKER_ID}")
    return snowflake

snowflake = init_snowflake(conf)

@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )


@app.get("/api/v1/guid")
async def guid():
    try:
        i = snowflake.next()
        return {"guid": i, "guid_str": str(i)}
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))
