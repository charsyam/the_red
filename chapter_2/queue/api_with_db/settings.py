from pydantic import BaseSettings


class Settings(BaseSettings):
    APP_ENDPOINT: str = 'localhost:8080'
    CONFIG_PATH: str = None
    DATABASE_URL: str = "mysql+pymysql://nadia:test123%40#@192.168.0.102:3306/nadia"

settings = Settings()

def get_settings():
    global settings
    return settings
