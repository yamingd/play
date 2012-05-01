# -*- coding: utf-8 -*-
from play.utils.functional import LazyObject, empty

class AppGlobal(object):
    """
    you can access:
    from play import app_global
    app_global.dbm
    app_global.redis
    app_global.cache
    app_global.search.person.add
    app_global.models.Person.find_byid(id)
    --------------------------------------
    dbm,redis,cache,search are the keys of settings
    """
    def __init__(self):
        self.fmp = {}
    
    def set(self, key, conf, func):
        try:
            self.__dict__[key] = func(conf)
        except Exception as e:
            print 'error:', key, conf, func
            raise e
        self.fmp[key] = (func, conf)
        
    def __getattr__(self, key):
        c = self.__dict__[key]
        if not c:
            func, conf = self.fmp[key]
            self.__dict__[key] = c = func(conf)
        return c
    
    def __iter__(self):
        for key in self.fmp:
            yield (key, self.__dict__[key])

class LazyAppGlobal(LazyObject):
    def _setup(self):
        self._wrapped = AppGlobal()
        
    @property
    def configured(self):
        return self._wrapped is not empty
        
app_global = LazyAppGlobal()
