from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError

import json

ZK_HOSTS = "127.0.0.1:2181"
ZK_PATH = "/the_red/my_service/redis/sidekiq"

zk = KazooClient(hosts=ZK_HOSTS)

zk.start()

data = {
    "primary": "127.0.0.1:16379",
    "secondary": ["127.0.0.1:16380"]
}


try:
    if zk.exists(ZK_PATH):
        zk.delete(ZK_PATH)
except Exception as e:
    print(str(e))
    print("")

zk.ensure_path(ZK_PATH)
zk.set(ZK_PATH, value=json.dumps(data).encode('utf-8'))
