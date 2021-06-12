from rule import RangeShardPolicy, RangeInfo
from utils import range_config_to_range_infos

import pytest


def test_range_config_to_range_info():
    test_data = """{
                    "0": {"start": 0, "end": 100, "host": "192.168.0.102:6379"},
                    "1": {"start": 100, "end": -1, "host": "192.168.0.103:6379"},
                }"""
    infos = range_config_to_range_infos(test_data) 
    info = RangeInfo(0, 100, "192.168.0.101:6379")

    assert len(infos) == 1
