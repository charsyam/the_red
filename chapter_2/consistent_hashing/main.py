from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from bs4 import BeautifulSoup
from datetime import datetime
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import logging
import json_logging
import urllib.parse
import httpx
import sys
import json
import traceback

from exceptions import UnicornException
from settings import Settings

from consistent_hash import ConsistentHash 
from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from zoo import init_kazoo
from config import Config

from redis_conn import RedisConnection


def refresh_shard_range(nodes):
    connections = {}
    if not nodes or len(nodes) == 0:
        print("There is no redis nodes")
        return

    ch_list = [] 
    for node in nodes:
        print("Node: ", node)
        parts = node.split(':')
        addr = f"{parts[1]}:{parts[2]}"
        nick = parts[0]
        conn = RedisConnection(addr)
        ch_list.append((addr, nick, conn))

    replica = 1
    ch = ConsistentHash(ch_list, replica)

    global g_ch
    g_ch = ch
    print("Finished refresh_shard_range")


app = FastAPI()
my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)
ZK_PATH = "/the_red/cache/redis/scrap"

init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)
zk = init_kazoo(conf.section("zookeeper")["hosts"], ZK_PATH, refresh_shard_range)

templates = Jinja2Templates(directory="templates/")


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


def get_conn(ch, key):
    if not g_ch:
        return None

    v = g_ch.get(key)
    return g_ch.continuum[v[0]][2].get_conn()


def store_to_cache(url: str, value: str):
    global g_ch

    key = f"url:{url}"
    conn = get_conn(g_ch, key)
    if not conn:
        return None

    conn.set(key, json.dumps(value))


def get_from_cache(url: str):
    global g_ch

    key = f"url:{url}"
    conn = get_conn(g_ch, key)
    if not conn:
        return None

    try:
        value = conn.get(key)
        if value:
            return json.loads(value.decode('utf-8'))
        else:
            return None
    except Exception as e:
        raise e
    

@app.get("/api/v1/scrap/")
async def scrap(url: str):
    try:
        url = urllib.parse.unquote(url)
        value = get_from_cache(url)
        if not value:
            print("Not Exist in Cache: ", url)
            body = await call_api(url)
            value = parse_opengraph(body)
            store_to_cache(url, value)
        else:
            print("Exist in Cache: ", url)

        return value
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        raise UnicornException(status=400, code=-20000, message=str(e))


def all_keys(conn):
    results = []
    for key in conn.scan_iter("*"):
        k = key.decode('utf-8')
        results.append((g_ch.hash_func(k), k))

    return sorted(results, key=lambda x: x[0])


@app.get("/demo")
async def demo(request: Request):
    global g_ch
    results = []
    for k,i,v,h,nick in g_ch.continuum:
        try:
            keys = all_keys(v.get_conn())
            results.append((nick, h, keys))
        except Exception as e:
            results.append((nick, h, []))

    return templates.TemplateResponse('demo.html', context={'request': request, 'results': results})
