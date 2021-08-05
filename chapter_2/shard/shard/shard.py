import typing

from redis_conn import RedisConnection


INFINITE = -1


class RangeInfo:
    def __init__(self, start: int, end: int, host: str):
        self.start = start
        self.end = end
        self.host = host

    def validate(self) -> bool:
        if not self.host:
            return False

        if self.start < 0:
            return False

        if self.end == 0:
            return False

        if self.end > 0 and self.end <= self.start:
            return False

        return True

    def get(self) -> str:
        return self.host


class ShardPolicy:
    pass


class RangeShardPolicy(ShardPolicy):
    def __init__(self, infos):
        length = len(infos) 

        current = infos[0]
        if current.validate() == False:
            raise Exception("RangeInfo is invalid")
        
        for i in range(1, length):
            prev = infos[i-1]
            current = infos[i]

            if current.validate() == False:
                raise Exception("RangeInfo is invalid")

            if prev.end != current.start:
                raise Exception("RangeInfo Order is invalid")

        self.infos = infos

    def getShardInfo(self, key: int) -> str:
        for info in self.infos:
            if info.end == INFINITE:
                return info.get()

            if info.start <= key < info.end:
                return info.get()

        return None


class RangeShardManager:
    def __init__(self, policy):
        self.policy = policy
        try:
            connections = {}
            for info in policy.infos:
                parts = info.host.split(':')
                conn = RedisConnection(f"{parts[1]}:{parts[2]}")
                connections[info.host] = conn

            self.connections = connections
        except Exception as e:
            print(str(e))
       
    def get_policy(self):
        return self.policy

    def get_conn_by_host(self, host: str):
        return self.connections[host].get_conn()

    def get_conn(self, key: int):
        host = self.policy.getShardInfo(key)
        return self.connections[host].get_conn()
