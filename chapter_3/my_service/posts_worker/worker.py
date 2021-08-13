import redis
import json

from simplekiq import KiqQueue
from simplekiq import EventBuilder
from simplekiq import Worker
from config import Config

import crud
import models
import database
import httpx
import sys
import traceback
from zoo import init_kazoo


conf = Config(sys.argv[1])


def get_db():
    return database.Session()


class MyEventWorker(Worker):
    def __init__(self, queue, failed_queue):
        super().__init__(queue, failed_queue)

    def on_event(self, event_type, value):
        user_id = value["user_id"]
        url = value["url"]
        user_id = value["user_id"]
        contents = value["contents"]
        post_id = value["post_id"]
        scrap = json.dumps(value["scrap"]) if "scrap" in value else {}

        post = crud.add(get_db(), user_id, post_id, contents, url, scrap)
        print("log :", event_type, value)


dbconf = Config("worker.ini").section("database")
conf = Config("worker.ini").section("sidekiq")
queue = KiqQueue(conf["host"], conf["queue"], True)
failed_queue = KiqQueue(conf["host"], conf["failed_queue"], True)

database.init_database(dbconf["url"])
worker = MyEventWorker(queue, failed_queue)


while True:
    worker.process(True)
