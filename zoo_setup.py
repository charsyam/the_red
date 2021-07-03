from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError

ZK_DATA_PATH = "/the_red/cache/redis/consistent_hash"
ZK_HOSTS = "192.168.0.101:2181,192.168.0.102:2181,192.168.0.103:2181"
zk = KazooClient(hosts=ZK_HOSTS)

zk.start()

nodes = [
    "/redis1:192.168.0.102:6379",
    "/redis2:192.168.0.102:6380",
    "/redis3:192.168.0.102:6381"
]

for node in nodes:
    zk.delete(ZK_DATA_PATH+node, recursive=True)

for node in nodes:
    zk.ensure_path(ZK_DATA_PATH+node)
