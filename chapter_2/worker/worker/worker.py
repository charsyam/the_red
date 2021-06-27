import redis
from simplekiq import KiqQueue
from simplekiq import EventBuilder
from simplekiq import Worker


class MyEventWorker(Worker):
    def __init__(self, queue, failed_queue):
        super().__init__(queue, failed_queue)

    def _process(self, event_type, value):
        print(event_type, value)


conn = redis.StrictRedis("192.168.0.102", 6379)
queue = KiqQueue(conn, "api_worker", True)
failed_queue = KiqQueue(conn, "api_failed", True)

worker = MyEventWorker(queue, failed_queue)

while True:
    worker.process(True)
