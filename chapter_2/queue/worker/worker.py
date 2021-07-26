import redis
import json

from simplekiq import KiqQueue
from simplekiq import EventBuilder
from simplekiq import Worker
from config import Config

import crud
import models
import database


def get_db():
    return database.Session()

class MyEventWorker(Worker):
    def __init__(self, queue, failed_queue):
        super().__init__(queue, failed_queue)

    def _process(self, event_type, value):
        crud.create_url(get_db(), value["url"])
        print(event_type, value)


dbconf = Config("worker.ini").section("database")
conf = Config("worker.ini").section("sidekiq")
conn = redis.StrictRedis(conf["host"], conf["port"])
queue = KiqQueue(conn, conf["queue"], True)
failed_queue = KiqQueue(conn, "api_failed", True)

database.init_database(dbconf["url"])
worker = MyEventWorker(queue, failed_queue)


while True:
    worker.process(True)
