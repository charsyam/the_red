from typing import Optional
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from exceptions import UnicornException

from model import Post
from rule import RangeShardPolicy
from utils import range_config_to_range_infos
from post import PostService

from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from zoo import init_kazoo
from config import Config
from settings import Settings

import json
import redis


def refresh_shard_range(data):
    if not data or len(data) == 0:
        print("There is data")
        return

    try:
        infos = range_config_to_range_infos(data)
        policy = RangeShardPolicy(infos)
    except Exception as e:
        print(str(e))
        return None

    connections = {}
    for info in policy.infos:
        url = f"redis://{info.host}/"
        conn = redis.from_url(url)
        connections[info.host] = conn

    global g_shardpolicy
    global g_connections
    g_shardpolicy = policy
    g_connections = connections
    print("Finished refresh_shard_range")


app = FastAPI()

ZK_DATA_PATH = "/the_red/storages/redis/shards/ranges"

g_shardpolicy = None
g_post_service = PostService()

my_settings = Settings()

conf = Config(my_settings.CONFIG_PATH)
init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)
zk = init_kazoo(conf.section("zookeeper")["hosts"], ZK_DATA_PATH, refresh_shard_range)


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )


@app.get("/api/v1/shards")
async def list_shards():
    global g_shardpolicy
    results = {} 
    i = 0
    for info in g_shardpolicy.infos:
        idx = str(i)
        results[idx] = {"start": info.start, "end": info.end, "host": info.host}
        i += 1

    return results


def get_conn_from_shard(key: int):
    global g_shardpolicy
    host = g_shardpolicy.getShardInfo(key)
    conn = g_connections[host]

    print(key, host)
    return conn


@app.get("/api/v1/posts/{user_id}/")
async def lists(user_id: int, last: int = -1, limit: int = 10):
    conn = get_conn_from_shard(user_id)
    values, next_id = g_post_service.list(conn, user_id, limit, last)
    return {"code": 0, "message": "Ok", "data": values, "next": next_id}


@app.get("/api/v1/posts/{user_id}/{post_id}")
async def get_post(user_id: int, post_id: int):
    conn = get_conn_from_shard(user_id)
    post = g_post_service.get(conn, user_id, post_id)
    if not post:
        raise UnicornException(404, -10001, "No Post: {post_id}") 

    return {"code": 0, "message": "Ok", "post_id": post["post_id"], "contents": post["contents"]}


@app.post("/api/v1/posts")
async def write_post(post: Post):
    pass
