from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cprofile.profiler import CProfileMiddleware
from pydantic import BaseModel
from datetime import datetime

from prometheus_fastapi_instrumentator import Instrumentator, metrics
from bs4 import BeautifulSoup
from settings import Settings
from config import Config

import logging
import json_logging
import http3
import sys

from random import random
from math import log
from exceptions import UnicornException

import redis
import simplejson as json
import urllib.parse
import time


app = FastAPI()

DELTA = 500
BETA = 1.0

json_logging.init_fastapi(enable_json=True)
json_logging.init_request_instrument(app)
logger = json_logging.get_request_logger()
logger.addHandler(logging.handlers.TimedRotatingFileHandler("scrap.log", when='h'))
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


def conn_to_redis(config):
    addr = config.section("redis")
    host = addr['host']
    port = addr['port']
    url = f"redis://{host}:{port}/"
    return redis.from_url(url)


rconn = conn_to_redis(config)


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


def set_cache(url, value, ttl=10):
    key = f"url:{url}"
    rconn.setex(key, ttl, json.dumps(value))


def xfetch(url):
    key = f"url:{url}"
    ttl_ms = rconn.pttl(key)
    if (ttl_ms == -2):
        return None

    v = None
    if (ttl_ms == -1):
        v = rconn.get(key)
    else:
        r1 = random()
        l1 = log(r1)
        c1 = abs(int(DELTA * BETA * l1))
        print(c1 > ttl_ms, ttl_ms, c1, r1, l1)
        if c1 < ttl_ms:
            v = rconn.get(key)

    if v:
        return json.loads(v.decode('utf-8'))

    return None


@app.get("/api/v1/scrap")
async def scrap(url: str):
    try:
        url = urllib.parse.unquote(url)
        results = xfetch(url)
        if results:
            return results

        body = await call_api(url)
        results = parse_opengraph(body)
        set_cache(url, results)
        return results
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))
