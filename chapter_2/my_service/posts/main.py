from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from datetime import datetime
import httpx
import random
import json
import asyncio
import database
import crud
import redis

import urllib.parse
import traceback
import sys

from exceptions import UnicornException
from settings import Settings
from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from zoo import init_kazoo
from config import Config

from simplekiq import KiqQueue
from simplekiq import EventBuilder


app = FastAPI()


my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)
init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)
zk = init_kazoo(conf.section("zookeeper")["hosts"], None, None)
ZK_PATH = conf.section("zookeeper")["path"]
ZK_SCRAP_PATH = f"{ZK_PATH}/scrap/nodes"
ZK_GUID_PATH = f"{ZK_PATH}/guid/nodes"

database.init_database(conf.section("database")["url"])

queue = KiqQueue(conf.section('sidekiq')['host'], conf.section('sidekiq')['queue'], True)
failed_queue = KiqQueue(conf.section('sidekiq')['host'], conf.section('sidekiq')['failed_queue'], True)

event_builder = EventBuilder(queue)

templates = Jinja2Templates(directory="templates/")
client = httpx.AsyncClient()


scrap_servers = None
scrap_idx = 0
guid_servers = None
guid_idx = 0


def get_db():
    db = database.Session()
    return db


@zk.ChildrenWatch(ZK_SCRAP_PATH)
def watch_children_scrap(children):
    global scrap_servers
    scrap_servers = children
    print("scrap: ", children)
    

@zk.ChildrenWatch(ZK_GUID_PATH)
def watch_children_guid(children):
    global guid_servers
    guid_servers = children
    print("guid: ", children)


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    traceback.print_exc(file=sys.stderr)
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )


def get_guid_host(idx: int = -1):
    global guid_idx
    size = len(guid_servers)
    if size == 0:
        raise UnicornException(code=-20003, message="No Guid Servers", status=500)

    if idx >= size:
        raise UnicornException(code=-20004, message="Invalid Guid Servers", status=500)

    if idx < 0:
        idx = guid_idx
        guid_idx += 1
        if guid_idx >= size:
            guid_idx = 0

    return guid_servers[idx]


def get_scrap_host(idx: int = -1):
    global scrap_idx
    size = len(scrap_servers)
    if size == 0:
        raise UnicornException(code=-20001, message="No Scrap Servers", status=500)

    if idx >= size:
        raise UnicornException(code=-20002, message="Invalid Scrap Servers", status=500)

    if idx < 0:
        idx = scrap_idx
        scrap_idx += 1
        if scrap_idx >= size:
            scrap_idx = 0

    return scrap_servers[idx]


async def get_scrap_info(url: str):
    host = get_scrap_host() 
    call_url = f"http://{host}/api/v1/scrap?url={url}"
    return await client.get(call_url)


async def get_guid_info():
    host = get_guid_host() 
    call_url = f"http://{host}/api/v1/guid"
    return await client.get(call_url)


@app.get("/api/v1/post")
async def post(contents: str = "", url: str = None):
    try:
        if url:
            url = urllib.parse.unquote(url)

        tasks = []
        start = datetime.utcnow()
        tasks.append(asyncio.create_task(get_guid_info()))

        if url:
            tasks.append(asyncio.create_task(get_scrap_info(url)))

        result = await asyncio.gather(*tasks)
        end = datetime.utcnow()
        print(end-start)

        post_id = None
        scrap = ""

        r1 = json.loads(result[0].text)
        if url:
            r2 = json.loads(result[1].text)
            scrap = r2["scrap"]

        post_id = r1["guid"]

        value = event_builder.emit("post", {"post_id": post_id, "contents": contents, "scrap": scrap, "url": url})
        queue.enqueue(value)
        print(value)
        return {"post_id": post_id, "contents": contents, "scrap": scrap}

    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))


@app.get("/api/v1/posts")
async def posts(f: int = -1):
    try:
        results = crud.list(get_db(), f)
        posts = []
        for result in results:
            posts.append({"post_id": result.post_id, "contents": result.contents, "url": result.url, "scrap": json.loads(result.scrap)})
        return posts

    except Exception as e:
        raise UnicornException(status=500, code=-30000, message=str(e))


@app.get("/demo")
async def demo(request: Request, f: int = -1):
    try:
        results = crud.list(get_db(), f)
        posts = []
        for result in results:
            posts.append({"post_id": result.post_id, "contents": result.contents, "url": result.url, "scrap": json.loads(result.scrap)})

        return templates.TemplateResponse('demo.html', context={'request': request, 'results': posts})
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))
