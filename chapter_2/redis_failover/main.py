from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import sys
import json


from datetime import datetime
from bs4 import BeautifulSoup

import httpx
import urllib.parse
import traceback

from exceptions import UnicornException
from settings import Settings
from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from zoo import init_kazoo
from config import Config
from redis_conn import RedisConnection


pconn = None

def refresh_storage(data, stat):
    if not data:
        print("no data")
        return

    print(data)
    hosts = json.loads(data.decode('utf-8'))
    print(hosts)
    h1 = hosts["primary"] 

    global pconn
    pconn = RedisConnection(h1)
    print("Finished refresh_storage")


app = FastAPI()

ZK_SCRAP_PATH = "/the_red/storage/posts"

my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)
init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)
zk = init_kazoo(conf.section("zookeeper")["hosts"], ZK_SCRAP_PATH, refresh_storage, children=False)


client = httpx.AsyncClient()


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )


@app.get("/api/v1/write/{user_id}")
async def write(user_id: int, value: str):
    try:
        key = f"k:{user_id}"
        pconn.get_conn().set(key, value)
        return {"user_id": user_id, "value": value}
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        raise UnicornException(status=400, code=-20000, message=str(e))


@app.get("/api/v1/get/{user_id}")
async def get(user_id: int):
    try:
        key = f"k:{user_id}"
        value = pconn.get_conn().get(key)
        
        result = None
        if value:
            result = value.decode('utf-8')

        return {"user_id": user_id, "value": result}

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        raise UnicornException(status=400, code=-20000, message=str(e))
