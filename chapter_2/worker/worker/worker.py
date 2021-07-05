import redis
from simplekiq import KiqQueue
from simplekiq import EventBuilder
from simplekiq import Worker
from config import Config


class MyEventWorker(Worker):
    def __init__(self, queue, failed_queue):
        super().__init__(queue, failed_queue)

    def _process(self, event_type, value):
        print(event_type, value)


conf = Config("worker.ini").section("sidekiq")
conn = redis.StrictRedis(conf["host"], conf["port"])
queue = KiqQueue(conn, "api_worker", True)
failed_queue = KiqQueue(conn, "api_failed", True)

worker = MyEventWorker(queue, failed_queue)

while True:
    worker.process(True)
