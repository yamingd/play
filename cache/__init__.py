# -*- coding: utf-8 -*-

class CacheBase(object):
    def __init__(self, servers,
                debug = False, noreply = False, no_block = True,
                num_clients = 10, namespace=None):
        pass
    
    def add(self, key, value, time = 0, herd=True):
        pass
    
    def get(self, key, default=None):
        pass
    
    def set(self, key, value, time=None, herd=True):
        pass
    
    def delete(self, key):
        pass
    
    def incr(self,key, delta=1):
        pass
    
    def decr(self,key, delta=1):
        pass
    
    def get_multi(self, keys):
        pass
    
    def set_multi(self, data, time=None, herd=True):
        pass
    
    def delete_multi(self, keys, time=0):
        pass
    
    def remove(self):
        pass
    
    def close(self):
        pass
    
    def clear(self):
        pass
    
    def stat(self):
        pass
    