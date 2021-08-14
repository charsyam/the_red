from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from datetime import datetime
import sys
import httpx
import socket

from bs4 import BeautifulSoup

import urllib.parse

from exceptions import UnicornException
from settings import Settings
from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from zoo import init_kazoo
from config import Config


app = FastAPI()


my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)
init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)
zk = init_kazoo(conf.section("zookeeper")["hosts"], None, None)
ZK_PATH = conf.section("zookeeper")["path"]


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    self_ip = s.getsockname()[0]
    s.close()
    return self_ip


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    import traceback
    traceback.print_exc(file=sys.stderr)
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


@app.get("/api/v1/scrap")
async def scrap(url: str):
    try:
        url = urllib.parse.unquote(url)
        body = await call_api(url)
        return parse_opengraph(body)
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))


def register_into_service_discovery(endpoint):
    node_path = f"{ZK_PATH}/{endpoint}"
    if zk.exists(node_path):
        zk.delete(node_path)
    zk.create(node_path, ephemeral=True, makepath=True)


@app.on_event("startup")
def startup():
    parts = my_settings.APP_ENDPOINT.split(":")
    local_ip = get_local_ip()

    endpoints = f"{local_ip}:{parts[1]}"
    register_into_service_discovery(endpoints)
