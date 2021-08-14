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
import time

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

from redis_conn import RedisConnection
from consistent_hash import ConsistentHash


app = FastAPI()

EPOCH = time.mktime((2021, 6, 1, 0, 0, 0, 0, 0, 0))

my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)
init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)
zk = init_kazoo(conf.section("zookeeper")["hosts"], None, None)
ZK_PATH = conf.section("zookeeper")["path"]
ZK_GUID_PATH = f"{ZK_PATH}/guid/nodes"
ZK_CACHE_PATH = f"{ZK_PATH}/cache/nodes"
ZK_SCRAP_PATH = f"{ZK_PATH}/scrap/nodes"

database.init_database(conf.section("database")["url"])

queue = KiqQueue(conf.section('sidekiq')['host'], conf.section('sidekiq')['queue'], True)
failed_queue = KiqQueue(conf.section('sidekiq')['host'], conf.section('sidekiq')['failed_queue'], True)

event_builder = EventBuilder(queue)

templates = Jinja2Templates(directory="templates/")


guid_servers = None
guid_idx = 0

scrap_servers = None
scrap_idx = 0

g_ch = None


def get_db():
    db = database.Session()
    return db


def get_timestamp():
    now = int(int((time.time()) * 1000) - EPOCH) << 22
    return now


def rehash_scrap_servers(nodes):
    global scrap_servers
    scrap_servers = nodes


def rehash_cache_servers(nodes):
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


@zk.ChildrenWatch(ZK_CACHE_PATH)
def watch_children_cache_nodes(children):
    rehash_cache_servers(children)


@zk.ChildrenWatch(ZK_SCRAP_PATH)
def watch_children_scrap_nodes(children):
    rehash_scrap_servers(children)
    

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
    async with httpx.AsyncClient() as client:
        host = get_scrap_host() 
        call_url = f"http://{host}/api/v1/scrap?url={url}"
        print(call_url)
        return await client.get(call_url)


async def get_guid_info():
    async with httpx.AsyncClient() as client:
        host = get_guid_host() 
        call_url = f"http://{host}/api/v1/guid"
        return await client.get(call_url)


def gen_scrap_key(url):
    return f"url:{url}"


def gen_user_list_key(uid):
    return f"pl:{uid}"


def gen_post_key(post_id):
    return f"post:{post_id}"


def get_conn(ch, key):
    if not g_ch:
        return None

    v = g_ch.get(key)
    return g_ch.continuum[v[0]][2].get_conn()


def cache_post_list(conn, uid, results):
    key = gen_user_list_key(uid)

    for r in results:
        post_id = r.post_id
        conn.zadd(key, {post_id: post_id})

    
def cache_scrap(url, scrap):
    key = gen_scrap_key(url)
    conn = get_conn(g_ch, key)
    conn.setex(key, 86400*2, json.dumps(scrap))


def cache_post(conn, post):
    post_key = gen_post_key(post.post_id)
    print("post: ", post)
    print("scrap: ", type(post.scrap))
    print("scrap2: ", model2post(post))
    conn.setex(post_key, 86400*7, json.dumps(model2post(post)))


def model2post(model):
    return {"user_id": model.user_id, "post_id": model.post_id, "contents": model.contents, "url": model.url, "scrap": json.loads(model.scrap)}
    

def try_fill_post_list_cache(user_id, limit=10):
    key = gen_user_list_key(user_id)
    conn = get_conn(g_ch, key)
    if not conn:
        return None

    if conn.exists(key):
        return None

    results, next_id = get_post_list_from_db(user_id, -1, limit)
    return fill_post_list_cache(conn, user_id, results)
    

def get_post_list_from_db(user_id, last, limit):
    if last == -1:
        last = get_timestamp()

    next_id = None
    results = crud.list(get_db(), user_id, last, limit+1)
    if len(results) == limit + 1:
        next_id = results[-1].post_id
        results = results[:limit]

    return results, next_id


def fill_post_list_cache(conn, user_id, results):
    cache_post_list(conn, uid, results)
    for r in results:
        cache_post(conn, r)
    

def store_to_cache(uid, post):
    global g_ch

    post_id = post.post_id
    key = gen_user_list_key(uid)
    conn = get_conn(g_ch, key)
    if not conn:
        return None

    cache_post_list(conn, uid, [post])
    cache_post(conn, post)


def get_from_cache(uid: int, last, limit=10):
    global g_ch

    key = gen_user_list_key(uid)
    conn = get_conn(g_ch, key)
    if not conn:
        return None

    try:
        values = conn.zrevrangebyscore(key, last, "-inf", start=0, num=limit+1)
        next_id = None

        if values:
            length = len(values)
            if len(values) == limit+1:
                next_id = values[-1].decode('utf-8')

            keys = [gen_post_key(v.decode('utf-8')) for v in values[:limit]]
            values = conn.mget(keys)
            results = zip(keys, values)
            return results, next_id
        else:
            return None, None
    except Exception as e:
        raise e 


def get_scrap_from_cache(url):
    key = gen_scrap_key(url)
    conn = get_conn(g_ch, key)
    
    scrap_raw = conn.get(key)
    if scrap_raw:
        return json.loads(scrap_raw.decode('utf-8'))
    else:
        return None


def get_post_ids_from_cache(conn, key, last, limit):
    try:
        print(key, last, limit)
        values = conn.zrevrangebyscore(key, last, "-inf", start=0, num=limit+1)
        if values:
            return [v.decode('utf-8') for v in values]
        else:
            return None 
    except Exception as e:
        raise e 
    

def compansate_ids(conn, uid, d_results, not_cached_ids):
    results = []

    for r in d_results:
        post = model2post(r)
        results.append(post)
        print(f"set not cached key: {post} in cache")
        cache_post(conn, r)
        not_cached_ids.remove(str(r.post_id))

    # Remove Non exist keys in Database
    key = gen_user_list_key(uid)
    for not_existed_id in not_cached_ids:
        print(f"Remove Non exist keys: {not_existed_id}")
        conn.zrem(key, not_existed_id)

    return results


def get_posts(uid: int, last, limit=10):
    global g_ch

    db = get_db()
    results = None
    next_id = None
    try:
        last_timestamp = last
        if last == -1:
            last_timestamp = get_timestamp()
            
        key = gen_user_list_key(uid)
        print(key, last_timestamp)
            
        conn = get_conn(g_ch, key)
        values = get_post_ids_from_cache(conn, key, last_timestamp, limit)
        print("cached ids: ", values)
        if not values:
            tmp_results, next_id = get_post_list_from_db(uid, last_timestamp, limit)
            if last == -1:
                cache_post_list(conn, uid, tmp_results)

            for r in tmp_results:
                cache_post(conn, r)

            results = [model2post(r) for r in tmp_results] 
        else:
            if len(values) == limit + 1:
                next_id = values[-1]
                values = values[:limit]

            tmp_results = []
            keys = [gen_post_key(v) for v in values[:limit]]
            c_results = conn.mget(keys)
            zipped_results = zip(values, c_results)
            not_cached_ids = []
            
            for r in zipped_results:
                if r[1] == None:
                    not_cached_ids.append(r[0])

            d_results = crud.posts(db, not_cached_ids)
            d_results = compansate_ids(conn, key, d_results, not_cached_ids)

            for r in c_results:
                if r:
                    tmp_results.append(json.loads(r.decode('utf-8')))

            for r in d_results:
                tmp_results.append(model2post(r))

            results = sorted(tmp_results, key= lambda x: x["post_id"], reverse=True)

        print("results: ", results)
        return results, next_id
    except Exception as e:
        raise e 



@app.get("/api/v1/write_post/{uid}")
async def write_post(uid: int, contents: str = "", url: str = None):
    try:
        scrap = {}
        if url:
            url = urllib.parse.unquote(url)
            scrap = get_scrap_from_cache(url)
            print(f"Cache hit [scrap]: {url}")

        tasks = []
        start = datetime.utcnow()

        tasks.append(asyncio.create_task(get_guid_info()))
        if url and not scrap:
            tasks.append(asyncio.create_task(get_scrap_info(url)))

        result = await asyncio.gather(*tasks)
        end = datetime.utcnow()

        post_id = None

        guid_result = json.loads(result[0].text)
        if url and not scrap:
            if result[1].status_code == 200:
                scrap_result = json.loads(result[1].text)
                scrap = scrap_result["scrap"]
                cache_scrap(url, scrap)
            else:
                scrap = {}

        post_id = guid_result["guid"]

        post = crud.create_post(user_id=uid, post_id=post_id, contents=contents, url=url, scrap=json.dumps(scrap))
        try_fill_post_list_cache(uid)
        store_to_cache(uid, post)

        value = event_builder.emit("update", model2post(post))
        queue.enqueue(value)
        return model2post(post)

    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))


@app.get("/api/v1/posts/{uid}")
async def posts(uid: int, f: int = -1):
    try:
        posts, next_id = get_posts(uid, f)
        return {"posts": posts, "next_id": next_id}

    except Exception as e:
        raise UnicornException(status=500, code=-30000, message=str(e))


@app.get("/demo")
async def demo(request: Request, user_id: int, f: int = -1):
    try:
        results, next_id = get_posts(user_id, f)
        if not results:
            return f"Not Existed User: {user_id}"
        return templates.TemplateResponse('demo.html', context={'request': request, 'results': results})
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))
