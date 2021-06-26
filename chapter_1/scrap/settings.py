from pydantic import BaseSettings


class Settings(BaseSettings):
    APP_ENDPOINT: str = 'localhost:8080'
