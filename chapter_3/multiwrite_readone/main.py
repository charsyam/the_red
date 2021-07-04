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
import http3
import sys
import json
import random

from exceptions import UnicornException
from settings import Settings

from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from zoo import init_kazoo
from config import Config


class MultiCache:
    def __init__(self, hosts, replica = 3):
        self.conns = []
        for host in hosts:
            url = f"redis://{host}/"
            conn = redis.from_url(url)
            self.conns.append(conn)

        self.replica = replica
        self.count = len(self.conns)

    def hash(self, key):
        key = key.encode('utf-8')
        return struct.unpack('<I', hashlib.md5(key).digest()[0:4])[0]

    def set(self, key, value):
        idx = self.hash(key) % self.count
        for i in range(self.replica):
            conn = self.conns[(idx+i) % self.count]
            conn.set(key, value)

    def get(self, key): 
        idx = self.hash(key) % self.count
        n = random.randint(0, self.replica - 1)
        conn = self.conns[(idx+n) % self.count]
        return conn.get(key)


def refresh_cache_hosts(nodes):
    connections = {}
    print(nodes)
    if not nodes or len(nodes) == 0:
        print("There is no redis nodes")
        return

    replica = 2
    ch = MultiCache(nodes, replica)

    global g_ch
    g_ch = ch
    print("Finished refresh_shard_range")


app = FastAPI()
settings = Settings()
conf = Config(settings.CONFIG_PATH)
ZK_DATA_PATH = "/the_red/cache/redis/scrap"

init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)
g_ch = None
zk = init_kazoo(conf.section("zookeeper")["hosts"], ZK_DATA_PATH, refresh_cache_hosts)

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


def set_to_cache(url: str, value: str):
    global g_ch

    key = f"url:{url}"
    g_ch.set(key, json.dumps(value))


def get_from_cache(url: str):
    global g_ch

    key = f"url:{url}"
    value = g_ch.get(key)
    if value:
        return json.loads(value.decode('utf-8'))

    return None
    

@app.get("/api/v1/scrap/")
async def scrap(url: str):
    try:
        url = urllib.parse.unquote(url)
        value = get_from_cache(url)
        if not value:
            body = await call_api(url)
            value = parse_opengraph(body)
            set_to_cache(url, value)

        return value
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))
