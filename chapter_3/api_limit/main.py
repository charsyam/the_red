from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from bs4 import BeautifulSoup
from datetime import datetime

import hashlib
import struct
import logging
import json_logging
import urllib.parse
import redis
import httpx
import sys
import json
import random
from datetime import datetime, timedelta

from exceptions import UnicornException
from settings import Settings

from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator


app = FastAPI()
settings = Settings()

init_cors(app)
init_instrumentator(app)

API_MAXIMUM_NUMBER = 10
N_MINUTES = 5
SECONDS = 60


rconn = redis.StrictRedis("127.0.0.1", 16379)


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


def gen_key_prefix(uid):
    return f"l:scrap:{uid}:"

def get_api_count(uid):
    keys = []
    now = datetime.now() 

    for i in range(N_MINUTES):
        key = gen_key_prefix(uid) + (now + timedelta(minutes=-1*i)).strftime("%Y%m%d%H%M")
        keys.append(key)

    values = rconn.mget(keys)

    s = 0
    for value in values:
        if value:
            s += int(value)    

    return s
    

def incr_api_count(uid):
    now = datetime.now() 
    key = gen_key_prefix(uid) + now.strftime("%Y%m%d%H%M")
    v = rconn.incrby(key)
    rconn.expire(key, N_MINUTES * SECONDS)
    return v


@app.get("/api/v1/scrap/")
async def scrap(uid: int, url: str):
    if not uid:
        raise UnicornException(status=401, code=-20001, message="Not Authrized user")

    count = get_api_count(uid)
    if count >= API_MAXIMUM_NUMBER:
        raise UnicornException(status=427, code=-20002, message="all limit exceeded error")

    try:
        incr_api_count(uid)
        url = urllib.parse.unquote(url)
        body = await call_api(url)
        value = parse_opengraph(body)
        value["api_count"] = count
        return value
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))
