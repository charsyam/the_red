from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError

ZK_DATA_PATH = "/the_red/storages/redis/shards/ranges"
ZK_HOSTS = "192.168.0.101:2181,192.168.0.102:2181,192.168.0.103:2181"
zk = KazooClient(hosts=ZK_HOSTS)

zk.start()

value = b"""{
            "0": {"start": 0, "end": 100, "host": "192.168.0.102:6379"}
         }"""
zk.set(ZK_DATA_PATH, value)
