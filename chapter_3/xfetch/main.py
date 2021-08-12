from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime

from bs4 import BeautifulSoup

import httpx
import sys

from random import random
from math import log

import simplejson as json
import urllib.parse
import time

from exceptions import UnicornException
from settings import Settings
from config import Config
from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from redis_conn import RedisConnection


app = FastAPI()

DELTA = 500
BETA = 1.0

my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)
init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)


def conn_to_redis(conf):
    addr = conf.section("redis")["host"]
    return RedisConnection(addr)


rconn = conn_to_redis(conf)


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )


async def call_api(url: str):
    async with httpx.AsyncClient() as client:
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

    resp = {}
    scrap = {}
    scrap["title"] = title["content"] if title else None
    scrap["url"] = url["content"] if url else None
    scrap["type"] = og_type["content"] if og_type else None
    scrap["image"] = image["content"] if image else None
    scrap["description"] = description["content"] if description else None
    scrap["author"] = author["content"] if author else None
    resp["scrap"] = scrap

    return resp


def set_cache(url, value, ttl=3):
    key = f"url:{url}"
    rconn.get_conn().setex(key, ttl, json.dumps(value))


def xfetch(url):
    conn = rconn.get_conn()
    key = f"url:{url}"
    ttl_ms = conn.pttl(key)
    if (ttl_ms == -2):
        return None

    v = None
    if (ttl_ms == -1):
        v = conn.get(key)
    else:
        r1 = random()
        l1 = log(r1)
        c1 = abs(int(DELTA * BETA * l1))
        print("c1 > ttl_ms: ", c1 > ttl_ms, "ttl_ms: ", ttl_ms, "c1: ", c1, "random: ", r1, "log(random()): ", l1)
        if c1 < ttl_ms:
            v = conn.get(key)

    if v:
        return json.loads(v.decode('utf-8'))

    return None


@app.get("/api/v1/scrap")
async def scrap(url: str):
    try:
        url = urllib.parse.unquote(url)
        results = xfetch(url)
        if results:
            print("Cache Hit: ", url)
            return results

        body = await call_api(url)
        results = parse_opengraph(body)
        print("Cache Miss: ", url)
        set_cache(url, results)
        return results
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))
