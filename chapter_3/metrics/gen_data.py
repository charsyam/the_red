import redis
import string
import random


keys = [
    "key1",
    "key2",
    "key3",
    "key4",
    "key5",
    "key6",
    "key7",
    "key8",
    "key9",
    "key10",
]

def gen_value(N):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

value = gen_value(10247680)
S_1MB = gen_value(1024768)
S_0_1MB = gen_value(1024*100)
S_0_0_1MB = gen_value(1024*10)

sizes = [
    1024,
    10240,
    102400,
    1024000
]


def gen_hash(conn, key, N):
    v = gen_value(N)
    for subkey in keys:
        conn.hset(key, subkey, v)


rconn = redis.from_url("redis://127.0.0.1:16379")


for key in keys:
    rconn.set(key, value) 


for i in range(2000):
    idx = random.randint(0,9)
    key = keys[idx]
    rconn.get(key)

for i in range(1000):
    idx = random.randint(0,9)
    key = f"h:{idx}"
    
    max_size_idx = len(sizes)
    v_idx = random.randint(0,max_size_idx-1)
    value = gen_value(sizes[v_idx])
    for sub_key in keys:
        rconn.hset(key, sub_key, value)


for i in range(1000):
    idx = random.randint(0,9)
    key = f"h:{idx}"
    rconn.hgetall(key)
