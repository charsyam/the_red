from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError


_callback = None
_zk = None

def init_kazoo(hosts, data_path, callback):
    global _zk
    global _callback

    _zk = KazooClient(hosts=hosts)
    _zk.start()

    _callback = callback

    print(data_path)
    @_zk.ChildrenWatch(data_path)
    def watch_refresh_shardrange(children):
#        print(data_path)
#        children = _zk.get_children(data_path)
        if _callback:
            _callback(children)

    return _zk


