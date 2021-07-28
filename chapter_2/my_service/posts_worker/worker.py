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
        url = value["url"]
        scrap = json.dumps(value["scrap"])
        contents = value["contents"]
        post_id = value["post_id"]

        print("scrap :", scrap)
        post = crud.create_post(post_id, contents, url, scrap)
        crud.insert(get_db(), post)
        print(event_type, value)


dbconf = Config("worker.ini").section("database")
conf = Config("worker.ini").section("sidekiq")
queue = KiqQueue(conf["host"], conf["queue"], True)
failed_queue = KiqQueue(conf["host"], conf["failed_queue"], True)

database.init_database(dbconf["url"])
worker = MyEventWorker(queue, failed_queue)


while True:
    worker.process(True)
