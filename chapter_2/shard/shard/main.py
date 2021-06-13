from typing import Optional
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from exceptions import UnicornException

from prometheus_fastapi_instrumentator import Instrumentator, metrics

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError

from model import Post
from rule import RangeShardPolicy
from utils import range_config_to_range_infos
from post import PostService

import json
import redis

app = FastAPI()

Instrumentator().instrument(app).expose(app)

ZK_DATA_PATH = "/the_red/storages/redis/shards/ranges"
ZK_HOSTS = "192.168.0.101:2181,192.168.0.102:2181,192.168.0.103:2181"
zk = KazooClient(hosts=ZK_HOSTS)

zk.start()

g_shardpolicy = None

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

g_post_service = PostService()

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


def refresh_shard_range(data):
    try:
        infos = range_config_to_range_infos(data.decode('utf-8')) 
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


@zk.DataWatch(ZK_DATA_PATH)
def watch_refresh_shardrange(data, stat):
    print("Data is %s" % data)
    print("Version is %s" % stat.version)

    refresh_shard_range(data)
