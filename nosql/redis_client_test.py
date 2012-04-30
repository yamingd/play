# -*- coding: utf-8 -*-
from shard_redis import SharedRedis, SharedRedisManager

conf = {'mod' : 128,
        'namespace': 'quora',
        'nodes' : {
            'redis_1' : '192.168.1.100:63790',
            'redis_2' : '192.168.1.100:63791',
            'redis_3' : '192.168.1.101:63790',
            'redis_4' : '192.168.1.101:63791'
        }}
proxy = SharedRedis(conf)

def basic_test():
    num = proxy.incr('basic_test',amount=2)
    num2 = proxy.get('basic_test')
    print 'incr get', num, num2
    
    num = proxy.decr('basic_test',amount=1)
    num2 = proxy.get('basic_test')
    print 'incr decr', num, num2
    
if __name__ == '__main__':
    basic_test()