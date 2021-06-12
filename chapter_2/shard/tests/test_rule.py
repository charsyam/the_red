from rule import RangeShardPolicy, RangeInfo

import pytest


def test_range_info_valid():
    info = RangeInfo(0, 100, "host1")
    assert info.validate() 

def test_range_info_invalid_start():
    info = RangeInfo(-1, 100, "host1")
    assert info.validate() == False

def test_range_info_invalid_end():
    info = RangeInfo(2, 0, "host1")
    assert info.validate() == False

def test_range_info_invalid_range():
    info = RangeInfo(100, 1, "host1")
    assert info.validate() == False

def test_range_shard_policy_valid():
    infos = [
        RangeInfo(0, 100, "host1"),
        RangeInfo(100, 200, "host2"),
        RangeInfo(200, 300, "host3"),
        RangeInfo(300, 400, "host4"),
    ]

    policy = RangeShardPolicy(infos)
    assert True

def test_range_shard_policy_valid():
    infos = [
        RangeInfo(0, 100, "host1"),
        RangeInfo(100, 200, "host2"),
        RangeInfo(200, 300, "host3"),
        RangeInfo(300, -1, "host4"),
    ]

    policy = RangeShardPolicy(infos)
    assert True

def test_range_shard_policy_invalid_infinite_range():
    infos = [
        RangeInfo(0, 100, "host1"),
        RangeInfo(50, 200, "host2"),
        RangeInfo(200, -1, "host3"),
        RangeInfo(300, 400, "host4"),
    ]

    with pytest.raises(Exception):
        policy = RangeShardPolicy(infos)

def test_range_shard_policy_invalid_overlap_range():
    infos = [
        RangeInfo(0, 100, "host1"),
        RangeInfo(50, 200, "host2"),
        RangeInfo(200, 300, "host3"),
        RangeInfo(300, 400, "host4"),
    ]

    with pytest.raises(Exception):
        policy = RangeShardPolicy(infos)

def test_range_shard_policy_invalid_skip_range():
    infos = [
        RangeInfo(0, 100, "host1"),
        RangeInfo(200, 300, "host3"),
        RangeInfo(300, 400, "host4"),
    ]

    with pytest.raises(Exception):
        policy = RangeShardPolicy(infos)

def test_range_shard_policy_valid_host():
    infos = [
        RangeInfo(0, 100, "host1"),
        RangeInfo(100, 200, "host2"),
        RangeInfo(200, 300, "host3"),
        RangeInfo(300, -1, "host4"),
    ]

    policy = RangeShardPolicy(infos)
    assert policy.getShardInfo(99) == "host1"
    assert policy.getShardInfo(100) == "host2"
    assert policy.getShardInfo(1000) == "host4"
