from typing import Optional
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from exceptions import UnicornException
import urllib.parse

import json
import redis
import httpx

from settings import Settings
from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from zoo import init_kazoo
from config import Config


def refresh_scrap(children):
    global scrap_endpoints
    scrap_endpoints = children
    print("Finished refresh_scrap")


app = FastAPI()
my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)

ZK_SCRAP_PATH = "/the_red/services/scrap/nodes"

init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)
zk = init_kazoo(conf.section("zookeeper")["hosts"], ZK_SCRAP_PATH, refresh_scrap)

client = httpx.AsyncClient()


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )


next_idx = 0
scrap_endpoints = []

async def call_api(url: str):
    global next_idx
    length = len(scrap_endpoints)
    if next_idx >= length:
        next_idx = 0

    endpoints = scrap_endpoints[next_idx]
    next_idx += 1
    encoded_url = urllib.parse.quote(url)
    url = f"http://{endpoints}/api/v1/scrap?url={encoded_url}"
    r = await client.get(url)
    return r.text
    

@app.get("/api/v1/posts/{user_id}/")
async def get_post(user_id: int, url: str):
    decoded_url = urllib.parse.unquote(url)
    scrap_raw = await call_api(decoded_url)
    scrap = json.loads(scrap_raw)
    return {"code": 0, "message": "Ok", "scarp": scrap}

