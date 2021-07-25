from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError


_callback = None
_zk = None

def init_kazoo(hosts, data_path, callback, children=True):
    global _zk
    global _callback

    _zk = KazooClient(hosts=hosts)
    _zk.start()

    _callback = callback

    if data_path:
        if children:
            @_zk.ChildrenWatch(data_path)
            def watch_children(children):
                print("Watch Children")
                if _callback:
                    _callback(children)
        else:
            @_zk.DataWatch(data_path)
            def watch_node(data, stat):
                print("Watch Node")
                if _callback:
                    _callback(data, stat)

    return _zk
