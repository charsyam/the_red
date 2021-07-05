from time import perf_counter

import redis
import sys


def pipeline(conn, n):
    t1_start = perf_counter()
    pipeline = conn.pipeline()
    for i in range(n):
        key = 'PR:%s'%(i)
        pipeline.set(key, key)
        if i%1024 == 0:
            pipeline.execute();

    t1_stop = perf_counter()
    print("Pipeline: Elapsed time:", t1_stop - t1_start, t1_stop, t1_start) 


def no_pipeline(conn,n):
    t1_start = perf_counter()
    for i in range(n):
        key = 'PR:%s'%(i)
        conn.set(key, key)

    t1_stop = perf_counter()
    print("No-Pipeline Elapsed time:", t1_stop - t1_start, t1_stop, t1_start) 


rconn = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

pipeline(rconn, int(sys.argv[1]))
no_pipeline(rconn, int(sys.argv[1]))
