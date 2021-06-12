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

@app.get("/api/v1/posts/{user_id}")
async def get_test(user_id: int):
    conn = get_conn_from_shard(user_id)
    key = f"key:{user_id}"

    value = conn.get(key)
    return {"code": 0, "message": "Ok", "data": value.decode('utf-8')}

@app.get("/api/v1/posts")
async def list_posts():
    pass

@app.post("/api/v1/posts")
async def write_post(post: Post):
    pass

@app.get("/api/v1/test/{user_id}")
async def write_test(user_id: int):
    conn = get_conn_from_shard(user_id)
    key = f"key:{user_id}"
    value = "test"
    conn.set(key, value)
    return {"code":0, "message": "Ok"}


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
