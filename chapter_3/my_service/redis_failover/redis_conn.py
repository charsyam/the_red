import redis


class RedisConnection:
    def __init__(self, host):
        self.pool = redis.ConnectionPool.from_url(f"redis://{host}")

    def get_conn(self):
        return redis.StrictRedis(connection_pool=self.pool)
