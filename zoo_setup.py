from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError

ZK_DATA_PATH = "/the_red/cache/redis/scrap"
ZK_HOSTS = "127.0.0.1:2181"
zk = KazooClient(hosts=ZK_HOSTS)

zk.start()

nodes = [
    "127.0.0.1:6379",
    "127.0.0.1:6380",
    "127.0.0.1:6381"
]

for node in nodes:
    zk.delete(ZK_DATA_PATH+"/"+node, recursive=True)

for node in nodes:
    zk.ensure_path(ZK_DATA_PATH+"/"+node)
