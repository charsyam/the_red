from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from exceptions import UnicornException

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError
from settings import Settings

import logging
import json_logging
from datetime import datetime
import http3

from prometheus_fastapi_instrumentator import Instrumentator, metrics
from bs4 import BeautifulSoup

import urllib.parse

app = FastAPI()
json_logging.init_fastapi(enable_json=True)
json_logging.init_request_instrument(app)

settings = Settings()

ZK_SCRAP_PATH = "/the_red/services/scrap/nodes"
ZK_HOSTS = "192.168.0.101:2181,192.168.0.102:2181,192.168.0.103:2181"
zk = KazooClient(hosts=ZK_HOSTS)


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

@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )

async def call_api(url: str):
    r = await client.get(url)
    return r.text


def parse_opengraph(body: str):
    soup = BeautifulSoup(body, 'html.parser')

    title = soup.find("meta",  {"property":"og:title"})
    url = soup.find("meta",  {"property":"og:url"})
    og_type = soup.find("meta",  {"property":"og:type"})
    image = soup.find("meta",  {"property":"og:image"})
    description = soup.find("meta",  {"property":"og:description"})
    author = soup.find("meta",  {"property":"og:article:author"})

    print(title)
    print(url)
    resp = {"code": 0}
    scrap = {}
    scrap["title"] = title["content"] if title else None
    scrap["url"] = url["content"] if url else None
    scrap["type"] = og_type["content"] if og_type else None
    scrap["image"] = image["content"] if image else None
    scrap["description"] = description["content"] if description else None
    scrap["author"] = author["content"] if author else None
    resp["scrap"] = scrap

    return resp

@app.get("/api/v1/scrap/")
#@cache()
async def scrap(url: str):
    try:
        print("url: ", url)
        url = urllib.parse.unquote(url)
        print("url: ", url)
        body = await call_api(url)
        return parse_opengraph(body)
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))

def register_into_service_discovery(endpoint):
    node_path = f"{ZK_SCRAP_PATH}/{endpoint}"
    zk.create(node_path, ephemeral=True, makepath=True)

@app.on_event("startup")
def startup():
    zk.start()
    register_into_service_discovery(settings.APP_ENDPOINT)
