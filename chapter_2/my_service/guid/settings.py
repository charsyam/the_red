from pydantic import BaseSettings


class Settings(BaseSettings):
    APP_ENDPOINT: str = 'localhost:8080'
    CONFIG_PATH: str = None
    DATACENTER_ID: int = 0
    WORKER_ID: int = 0
