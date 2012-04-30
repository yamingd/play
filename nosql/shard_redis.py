# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import datetime
import time

import redis

from play.utils.hash_ring import HashRing

class ShardNotSupportError(Exception):
    pass

class ShardNodeNotFoundError(Exception):
    pass

"""
our $redisConfig = {
    # node names
    'mod' => 128,
    'nodes' => {
        redis_1 : '192.168.1.100:63790',
        redis_2 : '192.168.1.100:63791',
        redis_3 : '192.168.1.101:63790',
        redis_4 : '192.168.1.101:63791'
    },
    # replication information
    'master_of' => {
        '192.168.1.100:63792' : '192.168.1.101:63790',
        '192.168.1.100:63793' : '192.168.1.101:63791',
        '192.168.1.101:63792' : '192.168.1.100:63790',
        '192.168.1.101:63793' : '192.168.1.100:63791',
    },
}
"""
class ShardRedisManager(object):
    def __init__(self,modbase,nodes,master_of):
        self.modbase = modbase
        self.nodes = nodes
        self.master_of = master_of
        self.pool = {}
        self.hr = None
        self.initpool()
        
    def initpool(self):
        for name in self.nodes:
            host,port = self.nodes[name].split(':')
            #print host,port
            r = redis.StrictRedis(host=host,port=int(port),db=0)
            self.pool[name] = r
        self.hr = HashRing(self.nodes.keys())
    
    def find(self,key):
        name = self.hr.get_node(key)
        if name:
            print 'mapping ', key, ' to ', name
            return self.pool[name]
        raise ShardNodeNotFoundError(key)
    
"""
http://blog.zawodny.com/2011/02/26/redis-sharding-at-craigslist/

Client-->ShardRedis-->Hash(Tag,Key)-->Node-->StrictRedis(Connection)-->Server

Command-Groups
"""
class ShardRedis(object):
    def __init__(self, conf):
        sm = ShardRedisManager(conf.mod,conf.nodes,{})
        self._nm = sm
        self.namespace = conf.namespace
        log.info('init ShardRedis')
        
    def _find_node(self,args):
        r = None
        node_key = None
        if isinstance(args, list):
            if len(args)<2:
                raise ShardNodeNotFoundError('Missing HashTag')
            node_key = args[0]
            name = args[1]
        else:
            name = args
            node_key = name
        r = self._nm.find(node_key)
        return r,name
    
    def pipeline(self, transaction=True, shard_hint=None):
        raise ShardNotSupportError("pipeline")

    def transaction(self, func, *watches, **kwargs):
        raise ShardNotSupportError("transaction")
    
    def lock(self, name, timeout=None, sleep=0.1):
        raise ShardNotSupportError("lock")

    #### SERVER INFORMATION ####
    def bgrewriteaof(self):
        raise ShardNotSupportError("bgrewriteaof")

    def bgsave(self):
        raise ShardNotSupportError("bgsave")

    def config_get(self, pattern="*"):
        raise ShardNotSupportError("config_get")

    def config_set(self, name, value):
        raise ShardNotSupportError("config_set")

    def dbsize(self):
        raise ShardNotSupportError("dbsize")

    def debug_object(self, key):
        r, name = self._find_node(key)
        return r.debug_object(name)

    def delete(self, *names):
        for name in names:
            r, key = self._find_node(name)
            r.delete(key)
            
    __delitem__ = delete

    def echo(self, value):
        raise ShardNotSupportError("echo")

    def flushall(self):
        raise ShardNotSupportError("flushall")

    def flushdb(self):
        raise ShardNotSupportError("flushdb")

    def info(self):
        raise ShardNotSupportError("info")

    def lastsave(self):
        raise ShardNotSupportError("lastsave")

    def object(self, infotype, key):
        r, key = self._find_node(key)
        return r.object(infotype, key)

    def ping(self):
        raise ShardNotSupportError("ping")

    def save(self):
        raise ShardNotSupportError("save")

    def shutdown(self):
        raise ShardNotSupportError("shutdown")

    def slaveof(self, host=None, port=None):
        raise ShardNotSupportError("slaveof")
    
    def akey(self, pattern, params):
        return self.namespace+':'+ pattern % params

    #### BASIC KEY COMMANDS ####
    def append(self, key, value):
        r, key = self._find_node(key)
        return r.append(key, value)

    def decr(self, name, amount=1):
        r, key = self._find_node(name)
        return r.decr(name, amount=amount)

    def exists(self, name):
        r, key = self._find_node(name)
        return r.exists(key)
        
    __contains__ = exists

    def expire(self, name, time):
        r, key = self._find_node(name)
        return r.expire(key, time)

    def expireat(self, name, when):
        r, key = self._find_node(name)
        return r.expireat(key, when)

    def get(self, name):
        r, key = self._find_node(name)
        return r.get(key)

    def __getitem__(self, name):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        value = self.get(name)
        if value:
            return value
        raise KeyError(name)

    def getbit(self, name, offset):
        r, key = self._find_node(name)
        return r.getbit(key, offset)

    def getset(self, name, value):
        r, key = self._find_node(name)
        return r.getset(key, value)

    def incr(self, name, amount=1):
        r, key = self._find_node(name)
        return r.incr(key, amount=amount)

    def keys(self, pattern='*'):
        raise ShardNotSupportError("keys")

    def mget(self, keys, *args):
        raise ShardNotSupportError("mget")

    def mset(self, mapping):
        raise ShardNotSupportError("mset")

    def msetnx(self, mapping):
        raise ShardNotSupportError("msetnx")

    def move(self, name, db):
        raise ShardNotSupportError("move")

    def persist(self, name):
        r, key = self._find_node(name)
        return r.persist(key)

    def randomkey(self):
        raise ShardNotSupportError("randomkey")

    def rename(self, src, dst):
        raise ShardNotSupportError("rename")

    def renamenx(self, src, dst):
        raise ShardNotSupportError("renamenx")

    def set(self, name, value):
        r, key = self._find_node(name)
        return r.set(key, value)
        
    __setitem__ = set

    def setbit(self, name, offset, value):
        r, key = self._find_node(name)
        return r.setbit(key, offset, value)

    def setex(self, name, time, value):
        r, key = self._find_node(name)
        return r.setex(key, time, value)

    def setnx(self, name, value):
        r, key = self._find_node(name)
        return r.setnx(key, value)

    def setrange(self, name, offset, value):
        r, key = self._find_node(name)
        return r.setrange(key, offset, value)

    def strlen(self, name):
        r, key = self._find_node(name)
        return r.strlen(key)

    def substr(self, name, start, end=-1):
        r, key = self._find_node(name)
        return r.substr(key, start, end=end)

    def ttl(self, name):
        r, key = self._find_node(name)
        return r.ttl(key)

    def type(self, name):
        r, key = self._find_node(name)
        return r.type(key)

    def watch(self, *names):
        raise ShardNotSupportError("watch")

    def unwatch(self):
        raise ShardNotSupportError("unwatch")

    #### LIST COMMANDS ####
    def blpop(self, keys, timeout=0):
        if len(keys)>2:
            raise ShardNotSupportError("blpop Too much keys, only two keys support")
        r, key = self._find_node(keys)
        return r.blpop(key, timeout=timeout)

    def brpop(self, keys, timeout=0):
        if len(keys)>2:
            raise ShardNotSupportError("brpop Too much keys, only two keys support")
        r, key = self._find_node(keys)
        return r.brpop(key, timeout=timeout)

    def brpoplpush(self, src, dst, timeout=0):
        raise ShardNotSupportError("brpoplpush")

    def lindex(self, name, index):
        r, key = self._find_node(name)
        return r.lindex(key, index)

    def linsert(self, name, where, refvalue, value):
        r, key = self._find_node(name)
        return r.linsert(key, where, refvalue, value)

    def llen(self, name):
        r, key = self._find_node(name)
        return r.llen(key)

    def lpop(self, name):
        r, key = self._find_node(name)
        return r.lpop(key)

    def lpush(self, name, *values):
        r, key = self._find_node(name)
        return r.lpush(key, *values)

    def lpushx(self, name, value):
        r, key = self._find_node(name)
        return r.lpushx(key, value)

    def lrange(self, name, start, end):
        r, key = self._find_node(name)
        return r.lrange(key, start, end)

    def lrem(self, name, count, value):
        r, key = self._find_node(name)
        return r.lrem(key, count, value)

    def lset(self, name, index, value):
        r, key = self._find_node(name)
        return r.lset(key, index, value)

    def ltrim(self, name, start, end):
        r, key = self._find_node(name)
        return r.ltrim(key, start, end)

    def rpop(self, name):
        r, key = self._find_node(name)
        return r.rpop(key)

    def rpoplpush(self, tag, src, dst):
        r, key = self._find_node(tag)
        return r.rpoplpush(src, dst)

    def rpush(self, name, *values):
        r, key = self._find_node(name)
        return r.rpush(key, *values)

    def rpushx(self, name, value):
        r, key = self._find_node(name)
        return r.rpushx(key, value)

    def sort(self, name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None):
        r, key = self._find_node(name)
        return r.sort(key, start=start, num=num, by=by, get=get,desc=desc,alpha=alpha,store=store)


    #### SET COMMANDS ####
    def sadd(self, name, *values):
        r, key = self._find_node(name)
        return r.sadd(key, *values)

    def scard(self, name):
        r, key = self._find_node(name)
        return r.scard(key)

    def sdiff(self, keys, *args):
        tag = keys[0]
        r, key = self._find_node(tag)
        return r.sdiff(keys[1:],*args)

    def sdiffstore(self, tag, dest, keys, *args):
        r, key = self._find_node(tag)
        return r.sdiffstore(dest, keys, *args)

    def sinter(self, tag, keys, *args):
        r, key = self._find_node(tag)
        return r.sinter(keys, *args)

    def sinterstore(self, tag, dest, keys, *args):
        r, key = self._find_node(tag)
        return r.sinterstore(dest, keys, *args)

    def sismember(self, name, value):
        r, key = self._find_node(name)
        return r.sismember(key, value)

    def smembers(self, name):
        r, key = self._find_node(name)
        return r.smembers(key)

    def smove(self, src, dst, value):
        raise ShardNotSupportError("smove src dst")

    def spop(self, name):
        r, key = self._find_node(name)
        return r.spop(key)

    def srandmember(self, name):
        r, key = self._find_node(name)
        return r.srandmember(key)

    def srem(self, name, *values):
        r, key = self._find_node(name)
        return r.srem(key, *values)

    def sunion(self, keys, *args):
        r, key = self._find_node(keys[0])
        return r.sunion(keys[1:],*args)

    def sunionstore(self, tag, dest, keys, *args):
        r, key = self._find_node(tag)
        return r.sunionstore(dest, keys, *args)

    #### SORTED SET COMMANDS ####
    def zadd(self, name, *args, **kwargs):
        r, key = self._find_node(name)
        return r.zadd(key, *args, **kwargs)

    def zcard(self, name):
        r, key = self._find_node(name)
        return r.zcard(key)

    def zcount(self, name, min, max):
        r, key = self._find_node(name)
        return r.zcount(key, min, max)

    def zincrby(self, name, value, amount=1):
        r, key = self._find_node(name)
        return r.zincrby(key, value, amount=amount)

    def zinterstore(self, tag, dest, keys, aggregate=None):
        r, key = self._find_node(tag)
        return r.zinterstore(dest, keys, aggregate=aggregate)

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float):
        r, key = self._find_node(name)
        return r.zrange(key, start, end, desc=desc, 
                        withscores=withscores, score_cast_func=score_cast_func)

    def zrangebyscore(self, name, min, max,
            start=None, num=None, withscores=False, score_cast_func=float):
        r, key = self._find_node(name)
        return r.zrangebyscore(key, min, max, start=start, num=num, 
                               withscores=withscores, score_cast_func=score_cast_func)

    def zrank(self, name, value):
        r, key = self._find_node(name)
        return r.zrank(key, value)

    def zrem(self, name, *values):
        r, key = self._find_node(name)
        return r.zrem(key, *values)

    def zremrangebyrank(self, name, min, max):
        r, key = self._find_node(name)
        return r.zremrangebyrank(key, min, max)

    def zremrangebyscore(self, name, min, max):
        r, key = self._find_node(name)
        return r.zremrangebyscore(key, min, max)

    def zrevrange(self, name, start, num, withscores=False,
                  score_cast_func=float):
        r, key = self._find_node(name)
        return r.zrevrange(key, start, num, 
                           withscores=withscores, score_cast_func=score_cast_func)

    def zrevrangebyscore(self, name, max, min,
            start=None, num=None, withscores=False, score_cast_func=float):
        r, key = self._find_node(name)
        return r.zrevrangebyscore(key, max, min, start=start, num=num, 
                                  withscores=withscores, score_cast_func=score_cast_func)

    def zrevrank(self, name, value):
        r, key = self._find_node(name)
        return r.zrevrank(key, value)

    def zscore(self, name, value):
        r, key = self._find_node(name)
        return r.zscore(key, value)

    def zunionstore(self, tag, dest, keys, aggregate=None):
        r, key = self._find_node(tag)
        return r.zunionstore(dest, keys, aggregate=aggregate)

    #### HASH COMMANDS ####
    def hdel(self, name, *keys):
        r, name = self._find_node(name)
        return r.hdel(name, *keys)

    def hexists(self, name, key):
        r, name = self._find_node(name)
        return r.hexists(name, key)

    def hget(self, name, key):
        r, name = self._find_node(name)
        return r.hget(name, key)

    def hgetall(self, name):
        r, name = self._find_node(name)
        return r.hgetall(name)

    def hincrby(self, name, key, amount=1):
        r, name = self._find_node(name)
        return r.hincrby(name, key, amount=amount)

    def hkeys(self, name):
        r, name = self._find_node(name)
        return r.hkeys(name)

    def hlen(self, name):
        r, name = self._find_node(name)
        return r.hlen(name)

    def hset(self, name, key, value):
        r, name = self._find_node(name)
        return r.hset(name, key, value)

    def hsetnx(self, name, key, value):
        r, name = self._find_node(name)
        return r.hsetnx(name, key, value)

    def hmset(self, name, mapping):
        r, name = self._find_node(name)
        return r.hmset(name, mapping)

    def hmget(self, name, keys):
        r, name = self._find_node(name)
        return r.hmget(name, keys)

    def hvals(self, name):
        r, name = self._find_node(name)
        return r.hvals(name)
    
    def publish(self, channel, message):
        raise ShardNotSupportError("publish")
    
    def pubsub(self, shard_hint=None):
        raise ShardNotSupportError("pubsub")

redis_proxy = ShardRedis