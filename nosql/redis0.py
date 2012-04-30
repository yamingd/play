# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import redis

class Redis(object):
    def __init__(self, conf):
        self.pool = redis.ConnectionPool()
        self.r = redis.Redis(host=conf.host,port=conf.port,connection_pool=self.pool)
        self.namespace = conf.namespace
        log.info('init Redis')
        
    def akey(self, pattern, params):
        return self.namespace+':'+ pattern % params
        
def instrument(method):
    def do(self, *args, **kwargs):
        try:
            key = args[0]
            assert isinstance(key, basestring)
        except:
            raise ValueError("method '%s' requires a key param as the first argument" % method)
        try:
            log.debug('%s(%s)' % (method, args))
            f = getattr(self.r, method)
            return f(*args, **kwargs)
        except:
            log.exception('unexpected error:%s',args)
            return None
    return do

for meth,f in redis.StrictRedis.__dict__.items():
    if meth.startswith('_'):
        continue
    setattr(Redis, meth, instrument(meth))

redis_proxy = Redis