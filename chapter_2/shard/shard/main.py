from typing import Optional
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from exceptions import UnicornException

from model import Post
from shard import RangeShardPolicy, RangeShardManager
from utils import range_config_to_range_infos
from post import PostService

from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from zoo import init_kazoo
from config import Config
from settings import Settings
from redis_conn import RedisConnection

import traceback
import json
import sys


g_shardmanager = None

def refresh_shard_range(data, stat):
    if not data:
        print("There is no data")
        return

    try:
        infos = range_config_to_range_infos(data)
        policy = RangeShardPolicy(infos)
    except Exception as e:
        print(str(e))
        return None

    global g_shardmanager
    shardmanager = RangeShardManager(policy)
    g_shardmanager = shardmanager
    print("Finished refresh_shard_range")


app = FastAPI()


g_post_service = PostService()

my_settings = Settings()

conf = Config(my_settings.CONFIG_PATH)
init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)
zk = init_kazoo(conf.section("zookeeper")["hosts"], conf.section("zookeeper")["path"], refresh_shard_range, False)

templates = Jinja2Templates(directory="templates/")


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )


def get_conn_from_shard(key: int):
    return g_shardmanager.get_conn(key)


@app.get("/api/v1/posts/{user_id}/")
async def lists(user_id: int, last: int = -1, limit: int = 10):
    conn = get_conn_from_shard(user_id)
    values, next_id = g_post_service.list(conn, user_id, limit, last)
    return {"data": values, "next": next_id}


@app.get("/api/v1/posts/{user_id}/{post_id}")
async def get_post(user_id: int, post_id: int):
    conn = get_conn_from_shard(user_id)
    post = g_post_service.get(conn, user_id, post_id)
    if not post:
        raise UnicornException(404, -10001, "No Post: {post_id}") 

    return {"post_id": post["post_id"], "contents": post["contents"]}


@app.get("/api/v1/write_post/{user_id}")
async def write_post(user_id: int, post_id: int, text: str):
    try:
        conn = get_conn_from_shard(user_id)
        post = g_post_service.write(conn, user_id, post_id, text)
        return {"post_id": post["post_id"], "contents": post["contents"]}
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        raise UnicornException(404, -10002, str(e)) 


def all_keys(conn):
    results = []
    for key in conn.scan_iter("*"):
        k = key.decode('utf-8')
        results.append(k)

    return sorted(results, key=lambda x: x[0])


@app.get("/demo")
async def demo(request: Request):
    policy = g_shardmanager.get_policy()
    results = []

    for info in policy.infos:
        r = f"{info.start} - {info.end}"
        try:
            conn = g_shardmanager.get_conn_by_host(info.host)
            print(conn)
            keys = all_keys(conn)
            results.append((info.host, r, keys))
            print(keys)
        except Exception as e:
            print(str(e))
            results.append((info.host, r, []))

    return templates.TemplateResponse('demo.html', context={'request': request, 'results': results})
