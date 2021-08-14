import time


def get_bitsize(v):
    return 1 << v

def get_timestamp():
    now = int((time.time()) * 1000)
    return now

def til_next_millis(last):
    timestamp = get_timestamp()
    while (timestamp <= last):
        time.sleep(0.001)
        timestamp = get_timestamp()

    return timestamp
