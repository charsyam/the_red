from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError

ZK_DATA_PATH = "/the_red/cache/redis/scrap"
ZK_HOSTS = "127.0.0.1:2181"
zk = KazooClient(hosts=ZK_HOSTS)

zk.start()

nodes = [
    "redis1:127.0.0.1:16379",
    "redis2:127.0.0.1:16380",
    "redis3:127.0.0.1:16381",
    "redis4:127.0.0.1:16382"
]

try:
    children = zk.get_children(ZK_DATA_PATH)
    for child in children:
        zk.delete(ZK_DATA_PATH+"/"+child, recursive=True)
        print("Deleted: " + ZK_DATA_PATH + "/" + child)
except Exception as e:
    print(str(e))
    print("")

for node in nodes:
    zk.ensure_path(ZK_DATA_PATH+"/"+node)
    print("Created: " + ZK_DATA_PATH + "/" + node)
