from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime

import logging
import json_logging
import httpx
import sys
import traceback

from exceptions import UnicornException
from settings import Settings
from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from config import Config
from models import CountryEntity

import geoip2.database
import geoip2


reader = geoip2.database.Reader('./GeoLite2-Country.mmdb')

app = FastAPI()
settings = Settings()


conf = Config(settings.CONFIG_PATH)

init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)


client = httpx.AsyncClient()


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )

@app.get("/api/v1/geoip/{ip}", response_model=CountryEntity)
async def read_item(ip: str):
    try:
        resp = reader.country(ip)
        return {"ip": ip, "country": resp.country.iso_code}
    except geoip2.errors.AddressNotFoundError as e:
        return {"ip": ip, "country": ""}
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        raise UnicornException(status=400, code=-20000, message=str(e))
