import redis
import os
import time
import json
import sys


from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError


ZK_HOSTS = "127.0.0.1:2181"
ZK_DATA_PATH = "/the_red/storage/posts"

zk = KazooClient(hosts=ZK_HOSTS)
zk.start()


pconn = None
sconns = []

@zk.DataWatch(ZK_DATA_PATH)
def watch_node(data, stat):
    if not data:
        print("There is no data")
        return

    hosts = json.loads(data.decode('utf-8'))
    print("Watch: ", hosts)
    h = hosts["primary"]

    conn = redis.from_url(f"redis://{h}/")
    global pconn
    pconn = conn


failure_count = 0



def ping(conn):
    try:
        conn.ping()
        return True
    except redis.exceptions.ConnectionError as e:
        print(str(e))
        return False
    

def get_good_secondary(hosts):
    for h in hosts:
         conn = redis.from_url(f"redis://{h}/")
         if ping(conn):
             return h, conn
    
    print("There is no good secondary", hosts)   
    return None, None
        

def monitor():
    global failure_count
    while True:
        print("monitor")
        if ping(pconn):
            failure_count = 0
            time.sleep(10)
        else:
            failure_count += 1
            if failure_count >= 3:
                data = zk.get(ZK_DATA_PATH)
                if not data:
                    print("There is no data")

                hosts = json.loads(data[0].decode('utf-8'))
                print("hosts: ", hosts)
                host, conn = get_good_secondary(hosts["secondary"])
                if not conn:
                    time.sleep(10)
                    continue

                old_p = hosts["primary"]
                hosts["secondary"].remove(host)
                hosts["secondary"].append(old_p)
                hosts["primary"] = host
                
                zk.set(ZK_DATA_PATH, json.dumps(hosts).encode('utf-8'))
                failure_count = 0
                print("redis stage changed: ", hosts)
                

monitor()
