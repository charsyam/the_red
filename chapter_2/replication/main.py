from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from datetime import datetime
import sys

from exceptions import UnicornException
from settings import Settings

import crud
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

templates = Jinja2Templates(directory="templates/")

database.init_database(conf.section("database")["url"])


def get_db():
    db = database.Session()
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


@app.get("/api/v1/list")
async def list(request: Request):
    try:
        results = crud.list(get_db())
        return templates.TemplateResponse('demo.html', context={'request': request, 'results': results})
    except Exception as e:
        raise UnicornException(status=400, code=-20000, message=str(e))
