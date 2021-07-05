from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from datetime import datetime
import httpx
import sys

from bs4 import BeautifulSoup

import urllib.parse
import redis

from sqlalchemy.orm import Session, sessionmaker
import sqlalchemy.orm.session

from exceptions import UnicornException
from settings import Settings

import crud
import models
import database

from log import init_log
from cors import init_cors
from instrumentator import init_instrumentator
from config import Config


app = FastAPI()


my_settings = Settings()
conf = Config(my_settings.CONFIG_PATH)

init_log(app, conf.section("log")["path"])
init_cors(app)
init_instrumentator(app)

client = httpx.AsyncClient()
models.Base.metadata.create_all(bind=database.engine)


def get_db():
    db = database.SessionLocal()
    return db


@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "message": exc.message},
    )


@app.get("/api/v1/url/")
async def scrap(url: str):
    try:
        ret = crud.create_url(get_db(), url)
        return ret
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))
