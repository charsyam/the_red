import json
from rule import RangeInfo

def range_config_to_range_infos(data):
    config = json.loads(data)
    length = len(config)

    infos = []
    for i in range(length):
        idx = str(i)
        infos.append(RangeInfo(config[idx]["start"], config[idx]["end"], config[idx]["host"]))

    return infos
