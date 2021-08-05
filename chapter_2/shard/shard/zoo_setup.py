from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError

import json

ZK_DATA_PARENT_PATH = "/the_red/storages/redis/shards"
ZK_DATA_PATH = ZK_DATA_PARENT_PATH + "/ranges"
ZK_HOSTS = "127.0.0.1:2181"
zk = KazooClient(hosts=ZK_HOSTS)

zk.start()

data = {
    "0": {"host": "redis0:127.0.0.1:16379", "start": 0, "end": 1000},
    "1": {"host": "redis1:127.0.0.1:16380", "start": 1000, "end": 2000},
    "2": {"host": "redis2:127.0.0.1:16381", "start": 2000, "end": 3500},
    "3": {"host": "redis3:127.0.0.1:16382", "start": 3500, "end": -1},
}


try:
    if zk.exists(ZK_DATA_PATH):
        zk.delete(ZK_DATA_PATH)
except Exception as e:
    print(str(e))
    print("")

zk.ensure_path(ZK_DATA_PARENT_PATH)
zk.create(ZK_DATA_PATH, value=json.dumps(data).encode('utf-8'))
