import typing


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
