from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError

import json

ZK_HOSTS = "127.0.0.1:2181"
ZK_QUEUE_PATH = "/the_red/my_service/queue/sidekiq"
ZK_CACHE_PATH = "/the_red/my_service/cache/nodes"
ZK_GUID_PATH = "/the_red/my_service/guid/nodes"
ZK_SCRAP_PATH = "/the_red/my_service/scrap/nodes"

zk = KazooClient(hosts=ZK_HOSTS)

zk.start()

queue = {
    "primary": "127.0.0.1:16379",
    "secondary": ["127.0.0.1:16380"]
}

caches = [
    "redis1:127.0.0.1:16380",
    "redis2:127.0.0.1:16381",
    "redis3:127.0.0.1:16382",
]


def create_nodes(path, nodes):
    try:
        children = zk.get_children(path)
        for child in children:
            zk.delete(path + "/" + child, recursive=True)
            print("Deleted: " + path + "/" + child)
    except Exception as e:
        print(str(e))
        print("")

    for node in nodes:
        zk.ensure_path(path + "/" + node)
        print("Created: " + path + "/" + node)


def create_data(path, data):
    try:
        if zk.exists(path):
            zk.delete(path)
    except Exception as e:
        print(str(e))
        print("")

    zk.ensure_path(path)
    zk.set(path, value=json.dumps(data).encode('utf-8'))


create_data(ZK_QUEUE_PATH, queue)
create_nodes(ZK_CACHE_PATH, caches)
