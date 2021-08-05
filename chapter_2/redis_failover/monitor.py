import redis
import os
import time
import json
import sys

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError

from config import Config
from zoo import init_kazoo


conf = Config(sys.argv[1])
ZK_PATH = conf.section("zookeeper")["path"]
print(ZK_PATH)
primary_addr = None


def connect_to_redis(host):
    try:
        return redis.from_url(f"redis://{host}/")
    except Exception as e:
        print(str(e))
        return None


def set_primary(host):
    try:
        rconn = connect_to_redis(host)
        rconn.slaveof()
        return rconn
    except Exception as e:
        print(str(e))
        return None


def info(conn):
    try:
        value = conn.info()
        return True, value
    except redis.exceptions.ConnectionError as e:
        print(str(e))
        return False, None


def set_replicas(primary, hosts):
    parts = primary.split(":")
    for h in hosts:
        rconn = connect_to_redis(h)
        ok, value = info(rconn)
        if ok:
            set_as_replica = False
            role = value["role"] 
            if role != "slave":
                set_as_replica = True
            else:
                master_host = value["master_host"]
                master_port = value["master_port"]
                master_addr = f"{master_host}:{master_port}"

                if master_addr != primary:
                    set_as_replica = True

            if set_as_replica:
                rconn.slaveof(parts[0], int(parts[1]))
                print(f"set {h} as replica of {primary}")


def refresh_node(data, stat):
    global primary_addr
    if not data:
        print("There is no data")
        return

    hosts = json.loads(data.decode('utf-8'))
    primary = hosts["primary"]

    primary_addr = primary
    conn = set_primary(primary)
    if conn:
        print(f"Primary Redis is {primary}")
        set_replicas(primary, hosts["secondary"])


zk = init_kazoo(conf.section("zookeeper")["hosts"], ZK_PATH, refresh_node, False)

failure_count = 0


def get_good_secondary(hosts):
    for h in hosts:
         conn = redis.from_url(f"redis://{h}/")
         if info(conn)[0]:
             return h, conn
    
    return None, None
        

def get_redis_info_from_zk(path):
    data = zk.get(ZK_PATH)
    if not data:
        return None

    return json.loads(data[0].decode('utf-8'))


def monitor():
    global failure_count
    global primary_addr

    while True:
        conn = connect_to_redis(primary_addr)
        ok, value = info(conn)
        if ok:
            failure_count = 0
            hosts = get_redis_info_from_zk(ZK_PATH)
            if hosts:
                if primary_addr == hosts["primary"]:
                    set_replicas(hosts["primary"], hosts["secondary"])

            time.sleep(10)
        else:
            print(f"check: {primary_addr} failure_count: {failure_count}")
            failure_count += 1
            if failure_count >= 3:
                hosts = get_redis_info_from_zk(ZK_PATH)
                if not hosts:
                    print("There is no data")
                    time.sleep(10)
                    continue

                host, conn = get_good_secondary(hosts["secondary"])
                if not conn:
                    print("There is no good secondary", hosts)
                    time.sleep(10)
                    continue

                old_p = hosts["primary"]
                hosts["secondary"].remove(host)
                hosts["secondary"].append(old_p)
                hosts["primary"] = host

                set_primary(host)
                zk.set(ZK_PATH, json.dumps(hosts).encode('utf-8'))

                failure_count = 0
                

monitor()
