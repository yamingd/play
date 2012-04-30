# -*- coding: utf-8 -*-

from play import app_global
from play.model import RowSet, RankSet

def prop_stat(model_clzz_name):
    """
    @prop_stat('UserStat')
    def stat(self): pass
    """
    def wrapper(*args, **kwargs):
        _self = args[0]
        if not hasattr(_self, '_stat'):
            _clzz = getattr(app_global.models, model_clzz_name)
            _self._stat = _clzz.find(getattr(_self,'id'))
        return _self._stat
    return wrapper

def prop_read(model_clzz_name, prop_name):
    """
    @prop_read('User', 'user_id'):
    def user(self): pass
    """
    def wrapper(*args, **kwargs):
        _self = args[0]
        _name = '_o_'+prop_name
        if not hasattr(_self, _name):
            _clzz = getattr(app_global.models, model_clzz_name)
            _ob = _clzz.find_byid(getattr(_self, prop_name))
            setattr(_self, _name, _ob)
        return getattr(_self, _name)
    return wrapper

def lrange(model_clazz_name, lrange_key):
    """
    @lrange('UserAttachment', 'user:attachs:%s'):
    def attachments(self, index=1, size=10): pass
    """
    def wrapper(*args, **kwargs):
        _self = args[0]
        pi = kwargs.get('index',1)
        ps = kwargs.get('size',10)
        _name = '_lr_'+model_clazz_name.lower()
        if not hasattr(_self, _name):
            _clzz = getattr(app_global.models, model_clzz_name)
            r = _clzz.g().redis
            key = r.namespace + ':' + lrange_key % _self.id
            start = (pi-1)*ps
            items = r.lrange(key, start, start+ps-1)
            total = r.llen(key)
            result = RowSet(items, _clzz, total=total, limit=ps, start=pi)
            setattr(_self, _name, result)
        return getattr(_self, _name)
    return wrapper

def zrange(model_clazz_name, field_name, zrange_key):
    """
    @zrange('UserAttachment', 'field', 'user:attachs:%s'):
    def attachments(self, index=1, size=10): pass
    """
    def wrapper(*args, **kwargs):
        _self = args[0]
        pi = kwargs.get('index',1)
        ps = kwargs.get('size',10)
        _name = '_zr_'+field_name
        if not hasattr(_self, _name):
            _clzz = getattr(app_global.models, model_clzz_name)
            r = _clzz.g().redis
            start = (pi-1)*ps
            key = r.namespace + ':' + zrange_key % _self.id
            items = r.zrange(key, start, start+ps-1, desc=False, withscores=True)
            result = RankSet(items, _clzz, field_name, limit=pageSize, start=pi)
            setattr(_self, _name, result)
        return getattr(_self, _name)
    return wrapper