from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel

class Post(BaseModel):
    user_id: int
    post_id: int
    text: str
