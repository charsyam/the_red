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


scrap_endpoints = []

def refresh_scrap(children):
    global scrap_endpoints
    scrap_endpoints = children
    for child in children:
        print(child)
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

async def call_api(url: str):
    global next_idx
    length = len(scrap_endpoints)
    if length == 0:
        raise UnicornException(code=-20003, status=500, message="No Scrap Server exist")

    if next_idx >= length:
        next_idx = 0

    endpoint = scrap_endpoints[next_idx]
    next_idx += 1
    encoded_url = urllib.parse.quote(url)
    url = f"http://{endpoint}/api/v1/scrap?url={encoded_url}"
    r = await client.get(url)
    return endpoint, r.text
    

@app.get("/api/v1/scrap/")
async def get_post(url: str):
    decoded_url = urllib.parse.unquote(url)
    endpoint, scrap_raw = await call_api(decoded_url)
    scrap = json.loads(scrap_raw)
    return {"endpoint": endpoint, "scarp": scrap}


@app.get("/list")
async def list():
    global scrap_endpoints
    return scrap_endpoints

