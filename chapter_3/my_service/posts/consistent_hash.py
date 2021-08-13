import sys
import hashlib
import struct

VALUE_IDX = 2
HASH_IDX = 3
LAST = -1
FIRST = 0

class ConsistentHash:
    def __init__(self, kvlist, replica, hash_func = None):
        self.hash_func = hash_func
        if not self.hash_func:
            self.hash_func = self.ketama_hash

        self.kvlist = kvlist
        self.replica = replica

        self.continuum = self.rebuild(kvlist)

    def ketama_hash(self, key):
        key = key.encode('utf-8')
        return struct.unpack('<I', hashlib.md5(key).digest()[0:4])[0]

      
    def rebuild(self, kvlist):
        continuum = [(k, i, v, self._hash("%s:%s"%(nick,i)), "%s:%s"%(nick, i)) \
                     for k, nick, v in kvlist \
                     for i in range(self.replica)]

        continuum.sort(key=lambda x: x[HASH_IDX])
        return continuum

    def _hash(self, key):
        return self.hash_func(key)

    def find_near_value(self, continnum, h):
        size = len(continnum)
        begin = left = 0
        end = right = size

        while left < right:
            middle = left + int((right - left) / 2)
            if continnum[middle][HASH_IDX] < h:
                left = middle + 1
            else:
                right = middle

        if right == end:
            right = begin

        return right, continnum[right][VALUE_IDX]
        

    def get(self, key):
        h = self._hash(key)
        if h < self.continuum[FIRST][HASH_IDX] or h > self.continuum[LAST][HASH_IDX]:
            return 0, self.continuum[FIRST][VALUE_IDX]

        return self.find_near_value(self.continuum, h)
      
      
if __name__ == "__main__":
    replica = 2
    kvlist = [
        ("host1", "cache1", "value1"),
        ("host2", "cache2", "value2"),
        ("host3", "cache3", "value3"),
        ("host4", "cache4", "value4")
    ]
    ch = ConsistentHash(kvlist, replica)
    v = ch.get(sys.argv[1])
    print(v[0], ch.continuum[v[0]])
