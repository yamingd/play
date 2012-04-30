# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import time
import traceback
import os,sys,re
import math

from play.utils import Storage
from play.utils import ipaddress, encoding, datefy
from play.db import Row
from play.conf import settings
from play import app_global as g

class NoneResult(object): pass

def _cache_key(clzz_name, args, prefix=''):
    """
    Make the cache key. We have to descend into *a and **kw to make
    sure that only regular strings are used in the key to keep 'foo'
    and u'foo' in an args list from resulting in differing keys
    """
    def _conv(s):
        if isinstance(s, str):
            return s
        elif isinstance(s, unicode):
            return encoding._force_utf8(s)
        elif isinstance(s, datetime):
            return s.strftime('%Y%m%d')
        else:
            return str(s)
    key = clzz_name
    if prefix:
        key = key +':'+prefix
    if args:
        key = key +':'+':'.join([_conv(x) for x in args if x])
    return key
    
class BaseModel(Row):
    table_name = None
    model_type = 'base'
    model_id = 0 # to dynamic locate model through op-id
    model_ops = [] #turn into ops classattr
    
    @classmethod
    def gen_pkid(clz):
        rkey = g.redis.akey('global:next%sId', (clz.__name__,))
        sid = g.redis.incr(rkey)
        return sid
    
    @classmethod
    def g(clz):
        return g
    
    @classmethod
    def mc(clz):
        return g.memcache
    
    @classmethod
    def db(clz):
        return g.dbm
    
    @classmethod
    def _get(clz, query, ident, **kwargs):
        _from_db = kwargs.get('update',False)
        _time = kwargs.get('time',600)
        cache_key = _cache_key(clz.__name__, ident)
        cache = clz.mc()
        obj = cache.get(cache_key) if not _from_db else None
        if obj is None:
            obj = clz.db().get(query,*ident, clz=clz)
            if obj is not None:
                cache.set(cache_key, obj, time=_time)
            else:
                cache.set(cache_key, NoneResult(), time=_time)
                return None
        if not isinstance(obj,NoneResult):
            return obj
        return None
    
    @classmethod
    def _get_unique(clz, query, ident, **kwargs):
        prefix = kwargs.get('prefix','unique')
        _from_db = kwargs.get('update',False)
        _time = kwargs.get('time',10)
        cache_key = _cache_key(clz.__name__, ident, prefix=prefix)
        cache = clz.mc()
        obj = cache.get(cache_key) if not _from_db else None
        if obj is None:
            obj = clz.db().get(query, *ident, clz=clz)
            if obj is not None:
                cache.set(cache_key, obj, time=_time)
            else:
                cache.set(cache_key, NoneResult(), time=_time)
                return None
        if not isinstance(obj,NoneResult):
            return obj
        return None
    
    @classmethod
    def _count(clz, query, ident, **kwargs):
        prefix = kwargs.get('prefix',None)
        _from_db = kwargs.get('update',False)
        _time = kwargs.get('time',600)
        cache_key = _cache_key(clz.__name__, ident, prefix=prefix)
        cache = clz.mc()
        obj = cache.get(cache_key) if not _from_db else None
        if obj is None:
            obj = clz.db().get(query, *ident)
            if obj is not None:
                cache.set(cache_key, obj, time=_time)
            else:
                cache.set(cache_key, NoneResult(), time=_time)
                return 0
        if not isinstance(obj,NoneResult):
            return obj
        return 0
        
    @classmethod
    def _fetch(clz, query, ident, **kwargs):
        ident = list(ident)
        prefix = kwargs.get('prefix',None)
        _from_db = kwargs.get('update',False)
        start = kwargs.get('start',None)
        limit = kwargs.get('limit',None)
        _time = kwargs.get('time',600)
        if start and limit:
            start = int(start)
            offset = (start-1)*limit
            ident.append(offset)
            ident.append(limit)
        cache_key = _cache_key(clz.__name__, ident, prefix=prefix)
        cache = clz.mc()
        sets = cache.get(cache_key) if not _from_db else None
        if sets is None:
            sets = clz.db().query(query, *ident, clz=clz)
            if sets:
                cache.set(cache_key, sets, _time)
            else:
                cache.set(cache_key, NoneResult(), 60)
                return []
        if not isinstance(sets,NoneResult):
            return sets
        return []
    
    @classmethod
    def expire_cache(clz,ident,prefix='',erase=True):
        cache_key = _cache_key(clz.__name__, ident, prefix=prefix)
        cache = clz.mc()
        if not erase:
            cache.set(cache_key, NoneResult(), time=10)
        else:
            cache.delete(cache_key)
    
    @classmethod
    def save(clz, sql, params):
        """
        insert or update
        """
        if sql and params:
            clz.db().execute(sql, *params)
        
    @classmethod
    def find_byid(clz, id):
        """
        For RowSet, table always has a column named id.
        """
        sql = 'select * from %s where id = %s' % (clz.table_name, '%s')
        return clz._get(sql, [id], time=86400)
    
    def to_solrdoc(self):
        """
        Convert to solr document
        """
        pass

class StatModel(BaseModel):
    stat_key = '%s:%s'
    model_type = 'stat'
    
    @classmethod
    def find(clz, id, _update=False):
        """
        读取用户操作统计数字
        """
        r = g.redis
        rkey = r.akey(clz.stat_key, (clz.__name__, id))
        stats = r.hgetall(rkey)
        if stats is None:
            return None
        m = {'id':id}
        for k,v in stats:
            m[k] = int(v)
        return clz(m)
    
    @classmethod
    def incr(clz, id, **kwargs):
        """
        更新操作统计数字
        """
        r = g.redis
        rkey = r.akey(clz.stat_key, (clz.__name__, id))
        for k in kwargs:
            r.hincrby(rkey, k, int(kwargs[k]))
        rkey = r.akey(clz.stat_key, (clz.__name__+':ts', datefy.today_str()))
        r.sadd(rkey, id)
    
    @classmethod
    def sync_to_db(clz, date=None):
        """
        同步内存更新回数据库.
        """
        r = g.redis
        date = date or datefy.yesterday()
        tsrkey = r.akey(clz.stat_key, (clz.__name__+':ts', datefy.format(date)))
        rids = r.smembers(tsrkey)
        if rids is None:
            return
        for aid in rids:
            rkey = r.akey(clz.stat_key, (clz.__name__, aid))
            stats = r.hgetall(rkey)
            if stats is None:
                continue 
            sql = " update %s set " % clz.table_name
            params = []
            sets = []
            for k,v in stats:
                sets.append(" %s=%s " % (k,'%s'))
                params.append(int(v))
            sql = sql + string.join(sets,', ') + ' where id = %s'
            params.append(aid)
            clz.db().execute(sql,*params)
        r.delete(tsrkey)

class RelationModel(BaseModel):
    model_type = 'relation'
    relation_1 = ('t1', 't2', 'column1')
    
    def add(clz):
        pass
    
    def remove(clz):
        pass
    
    def find(clz):
        pass
    