# -*- coding: utf-8 -*-
import jsonlib2 as json
from datetime import datetime

def _sa_to_dict(obj):
    for item in obj.__dict__.items():
        if item[0][0] is '_':
            continue
        if isinstance(item[1], str):
            yield [item[0], item[1].decode()]
        elif isinstance(item[1],datetime):
            yield [item[0], item[1].strftime('%Y-%m-%d %H:%M:%S')]
        else:
            yield item

def sa_as_json(obj):
    if isinstance(obj, list):
        return json.dumps(map(dict, map(_sa_to_dict, obj)))
    else:
        return json.dumps(dict(_sa_to_dict(obj)))
    
def dumps_sa(obj):
    return sa_as_json(obj)

def dumps(obj):
    return json.dumps(obj)

def loads(jstr):
    return json.loads(jstr)