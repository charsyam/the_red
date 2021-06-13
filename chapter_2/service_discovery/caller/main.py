from typing import Optional
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from exceptions import UnicornException
import urllib.parse

from prometheus_fastapi_instrumentator import Instrumentator, metrics

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError

import json
import redis
import httpx

app = FastAPI()

Instrumentator().instrument(app).expose(app)

ZK_POST_PATH = "/the_red/services/post/nodes"
ZK_SCRAP_PATH = "/the_red/services/scrap/nodes"
ZK_HOSTS = "192.168.0.101:2181,192.168.0.102:2181,192.168.0.103:2181"
zk = KazooClient(hosts=ZK_HOSTS)

client = httpx.AsyncClient()

zk.start()

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

    print("next_idx", next_idx)
    endpoints = scrap_endpoints[next_idx]
    next_idx += 1
    print("endpoints: ", endpoints)
    print("url: ", url)
    encoded_url = urllib.parse.quote(url)
    url = f"http://{endpoints}/api/v1/scrap?url={encoded_url}"
    print(url)
    r = await client.get(url)
    return r.text
    

@app.get("/api/v1/posts/{user_id}/")
async def get_post(user_id: int, url: str):
    decoded_url = urllib.parse.unquote(url)
    scrap_raw = await call_api(decoded_url)
    print("scrap_raw", scrap_raw)
    scrap = json.loads(scrap_raw)
    return {"code": 0, "message": "Ok", "scarp": scrap}

def refresh_scrap(children):
    for child in children:
        print(child)

    global scrap_endpoints
    scrap_endpoints = children
    print("Finished refresh_scrap")


@zk.ChildrenWatch(ZK_SCRAP_PATH)
def watch_refresh_scrap(children):
    refresh_scrap(children)
