from utils import get_timestamp, til_next_millis, get_bitsize
import time

EPOCH = time.mktime((2021, 6, 1, 0, 0, 0, 0, 0, 0))


class GUID_BITS:
    WORKER_ID_BITS = 5
    DATACENTER_ID_BITS = 5
    SEQUENCE_BITS = 12

    WORKER_ID_SHIFT = SEQUENCE_BITS
    DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS
    TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS

    WORKER_ID_MASK = (1 << WORKER_ID_BITS) - 1
    SEQUENCE_MASK = (1 << SEQUENCE_BITS) - 1


class Snowflake(object):
    def __init__(self, datacenter_id, worker_id, epoch = EPOCH):
        if datacenter_id >= get_bitsize(GUID_BITS.DATACENTER_ID_BITS) or \
           worker_id >= get_bitsize(GUID_BITS.WORKER_ID_BITS):
            raise Exception("Invalid datacenter_id or worker_id")

        self.epoch = epoch
        self.datacenter_id = (datacenter_id & GUID_BITS.DATACENTER_ID_BITS) << \
                              GUID_BITS.DATACENTER_ID_SHIFT
        self.worker_id = (worker_id & GUID_BITS.WORKER_ID_BITS) << \
                          GUID_BITS.WORKER_ID_SHIFT

        self.last_timestamp = -1
        self.sequence = 0

    def next(self):
        timestamp = get_timestamp()
        if (timestamp < self.last_timestamp):
            raise Exception("Clock moved backwards")

        if (timestamp == self.last_timestamp):
            self.sequence = (self.sequence + 1) & GUID_BITS.SEQUENCE_MASK
            if (self.sequence == 0):
                timestamp = til_next_millis(self.last_timestamp)
            else:
                self.sequence = 0

        self.last_timestamp = timestamp
        timestamp = timestamp - (int(self.epoch*1000))
        guoidValue = (timestamp << GUID_BITS.TIMESTAMP_LEFT_SHIFT) |\
                     (self.datacenter_id | (self.worker_id) | self.sequence)

        return guoidValue

