from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from bs4 import BeautifulSoup
from datetime import datetime
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import hashlib
import struct
import logging
import json_logging
import urllib.parse
import httpx
import sys
import json
import random
import mmh3
import traceback

from exceptions import UnicornException
from settings import Settings

from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from zoo import init_kazoo
from config import Config
from redis_conn import RedisConnection


class MultiCache:
    def __init__(self, hosts, replica = 3):
        self.conns = []
        for host in hosts:
            print("Node: ", host)
            parts = host.split(':')
            addr = f"{parts[1]}:{parts[2]}"
            nick = parts[0]
            conn = RedisConnection(addr)
            self.conns.append((host, conn))

        self.replica = replica
        self.count = len(self.conns)

    def hash(self, key):
        key = key.encode('utf-8')
        return mmh3.hash(key)

    def set(self, key, value):
        idx =self.hash(key) % self.count
        for i in range(self.replica):
            conn = self.conns[(idx+i) % self.count][1].get_conn()
            conn.set(key, value)

    def get_read_idx(self, h):
        n = random.randint(0, self.replica - 1)
        idx = ((h % self.count) + n) % self.count
        return idx

    def get(self, key): 
        idx = self.get_read_idx(self.hash(key))
        conn = self.conns[idx][1].get_conn()
        return idx, conn.get(key)


def refresh_cache_hosts(data, stat):
    print("data: ", data)
    nodes = json.loads(data.decode('utf-8'))
    if not nodes or len(nodes) == 0:
        print("There is no redis nodes")
        return

    
    replica = 2
    mc = MultiCache(nodes, replica)

    global g_mc
    g_mc = mc
    print("Finished refresh redis nodes")


app = FastAPI()
settings = Settings()
conf = Config(settings.CONFIG_PATH)
ZK_DATA_PATH = "/the_red/cache/redis/scrap"

init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)
g_mc = None
zk = init_kazoo(conf.section("zookeeper")["hosts"], ZK_DATA_PATH, refresh_cache_hosts, False)

client = httpx.AsyncClient()
templates = Jinja2Templates(directory="templates/")


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    traceback.print_exc(file=sys.stderr)
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


def set_to_cache(url: str, value: str):
    global g_mc

    key = f"url:{url}"
    g_mc.set(key, json.dumps(value))


def get_from_cache(url: str):
    global g_mc

    key = f"url:{url}"
    idx, value = g_mc.get(key)
    if value:
        return idx, json.loads(value.decode('utf-8'))

    return None, None
    

@app.get("/api/v1/scrap/")
async def scrap(url: str):
    try:
        url = urllib.parse.unquote(url)
        idx, value = get_from_cache(url)
        if not value:
            body = await call_api(url)
            value = parse_opengraph(body)
            set_to_cache(url, value)

        value["idx"] = idx
        return value
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))


def all_keys(conn):
    results = []
    for key in conn.scan_iter("*"):
        k = key.decode('utf-8')
        results.append(k)

    return sorted(results, key=lambda x: x)


@app.get("/demo")
async def demo(request: Request):
    global g_mc
    results = []
    for host, conn in g_mc.conns:
        try:
            keys = all_keys(conn.get_conn())
            results.append((host, keys))
        except Exception as e:
            results.append((host, []))

    return templates.TemplateResponse('demo.html', context={'request': request, 'results': results})
