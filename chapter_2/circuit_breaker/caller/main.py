from typing import Optional
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from exceptions import UnicornException
from datetime import timedelta

import urllib.parse
import json
import redis
import httpx

from settings import Settings
from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from config import Config

import aiobreaker 


app = FastAPI()
my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)

init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)


cb = aiobreaker.CircuitBreaker(fail_max=3, timeout_duration=timedelta(seconds=20))


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )


@cb
async def call_api(url: str):
    endpoint = conf.section("scrap")["endpoint"]
    encoded_url = urllib.parse.quote(url)
    url = f"http://{endpoint}/api/v1/scrap?url={encoded_url}"
    async with httpx.AsyncClient(timeout=2) as client:
        r = await client.get(url)
        return endpoint, r.text
    

@app.get("/api/v1/scrap/")
async def get_post(url: str):
    decoded_url = urllib.parse.unquote(url)
    try:
        endpoint, scrap_raw = await call_api(decoded_url)
        scrap = json.loads(scrap_raw)
        return {"code": 0, "message": "Ok", "endpoint": endpoint, "scarp": scrap}
    except aiobreaker.state.CircuitBreakerError as e:
        raise UnicornException(status=500, code=-20005, message="CircuitBreakerError")
