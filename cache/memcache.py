# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import time

from play.cache import CacheBase

import pylibmc as mclib
NotFoundError = mclib.NotFound

CACHE_HERD_TIMEOUT = 60
DEFAULT_TIMEOUT = 300

class Marker(object):
    pass

MARKER = Marker()

def key_func(key):
    return str(key)
    
class Memcache(CacheBase):
    def __init__(self,servers, debug = False, noreply = False, no_block = True,
                num_clients = 10, namespace=None):
        self.servers = servers
        self.namespace = namespace
        self.clients = mclib.ClientPool(n_slots = num_clients)
        for x in xrange(num_clients):
            client = mclib.Client(servers, binary=True)
            behaviors = {
                'no_block': no_block, # use async I/O
                'tcp_nodelay': True, # no nagle
                '_noreply': int(noreply),
                'ketama': True, # consistent hashing, Setting this behavior to True is a shortcut for setting "hash" to "md5" and "distribution" to "consistent ketama".
                }
            client.behaviors.update(behaviors)
            self.clients.put(client)
        log.info('init memcache client: %s' % servers)
        self.min_compress_len = 512*1024
    
    def _get_memcache_timeout(self, timeout):
        """
        Memcached deals with long (> 30 days) timeouts in a special
        way. Call this function to obtain a safe value for your timeout.
        """
        timeout = timeout or DEFAULT_TIMEOUT
        if timeout > 2592000: # 60*60*24*30, 30 days
            # See http://code.google.com/p/memcached/wiki/FAQ
            # "You can set expire times up to 30 days in the future. After that
            # memcached interprets it as a date, and will expire the item after
            # said date. This is a simple (but obscure) mechanic."
            #
            # This means that we have to switch to absolute timestamps.
            timeout += int(time.time())
        return timeout
    
    def _pack_value(self, value, timeout):
        """
        Packs a value to include a marker (to indicate that it's a packed
        value), the value itself, and the value's timeout information.
        """
        herd_timeout = (timeout or DEFAULT_TIMEOUT) + int(time.time())
        return (MARKER, value, herd_timeout)
    
    def _unpack_value(self, value, default=None):
        """
        Unpacks a value and returns a tuple whose first element is the value,
        and whose second element is whether it needs to be herd refreshed.
        """
        try:
            marker, unpacked, herd_timeout = value
        except (ValueError, TypeError):
            return value, False
        if not isinstance(marker, Marker):
            return value, False
        if herd_timeout < int(time.time()):
            return unpacked, True
        return unpacked, False

    def add(self, key, value, time = 0, herd=True):
        try:
            with self.clients.reserve() as mc:
                # If the user chooses to use the herd mechanism, then encode some
                # timestamp information into the object to be persisted into memcached
                if herd and time != 0:
                    packed = self._pack_value(value, time)
                    real_timeout = (self._get_memcache_timeout(time) +
                        CACHE_HERD_TIMEOUT)
                else:
                    packed = value
                    real_timeout = self._get_memcache_timeout(timeout)
                return mc.add(key_func(key), packed, real_timeout)
        except:
            log.exception('unexpected error:%s' % key)
            return None
    
    def get(self, key, default=None):
        try:
            encoded_key = key_func(key)
            with self.clients.reserve() as mc:
                packed = mc.get(encoded_key)
                if packed is None:
                    return default
                val, refresh = self._unpack_value(packed)
                # If the cache has expired according to the embedded timeout, then
                # shove it back into the cache for a while, but act as if it was a
                # cache miss.
                if refresh:
                    mc.set(encoded_key, val, 
                            self._get_memcache_timeout(CACHE_HERD_TIMEOUT))
                    return default
                return val
        except:
            log.exception('unexpected error:%s' % key)
            return None
    
    def _set(self, mc, key, value, time=None, herd=True):
        if herd and time != 0:
            packed = self._pack_value(value, time)
            real_timeout = (self._get_memcache_timeout(time) +
                CACHE_HERD_TIMEOUT)
        else:
            packed = value
            real_timeout = self._get_memcache_timeout(time)
        return mc.set(key_func(key), packed, real_timeout)
    
    def set(self, key, value, time=None, herd=True):
        # If the user chooses to use the herd mechanism, then encode some
        # timestamp information into the object to be persisted into memcached
        try:
            with self.clients.reserve() as mc:
                self._set(mc,key,value,time=time,herd=herd)
        except:
            log.exception('unexpected error:%s' % key)
            return None
        
    def delete(self, key):
        try:
            with self.clients.reserve() as mc:
                log.debug("delete cache %s" % key)
                return mc.delete(key_func(key))
        except:
            log.exception('unexpected error:%s' % key)
            return None
        
    def incr(self,key, delta=1):
        try:
            with self.clients.reserve() as mc:
                ret = mc.incr(key_func(key), delta)
                log.debug("incr %s (delta=%s) ret=%s" % (key,delta,ret))
                return ret
        except:
            log.exception('unexpected error:%s' % key)
            return None
        
    def decr(self,key, delta=1):
        try:
            with self.clients.reserve() as mc:
                ret = mc.decr(key_func(key), delta)
                log.debug("decr %s (delta=%s) %s" % (key,delta,ret))
                return ret
        except:
            log.exception('unexpected error:%s' % key)
            return None
    
    def get_multi(self, keys):
        if not keys:
            return {}
        # First, map all of the keys through our key function
        rvals = map(key_func, keys)
        try:
            with self.clients.reserve() as mc:
                packed_resp = mc.get_multi(rvals)
                
                resp = {}
                reinsert = {}
                
                for key, packed in packed_resp.iteritems():
                    # If it was a miss, treat it as a miss to our response & continue
                    if packed is None:
                        resp[key] = packed
                        continue
                    
                    val, refresh = self._unpack_value(packed)
                    if refresh:
                        reinsert[key] = val
                        resp[key] = None
                    else:
                        resp[key] = val
                
                # If there are values to re-insert for a short period of time, then do
                # so now.
                if reinsert:
                    mc.set_multi(reinsert,
                        self._get_memcache_timeout(CACHE_HERD_TIMEOUT))
                
                # Build a reverse map of encoded keys to the original keys, so that
                # the returned dict's keys are what users expect (in that they match
                # what the user originally entered)
                reverse = dict(zip(rvals, keys))
                
                return dict(((reverse[k], v) for k, v in resp.iteritems()))
        except:
            log.exception('unexpected error')
            
    def set_multi(self, data, time=None, herd=True):
        try:
            with self.clients.reserve() as mc:
                if herd and time != 0:
                    safe_data = dict(((key_func(k), self._pack_value(v, time))
                        for k, v in data.iteritems()))
                else:
                    safe_data = dict((
                        (key_func(k), v) for k, v in data.iteritems()))
                mc.set_multi(safe_data, self._get_memcache_timeout(time))
        except:
            log.exception('unexpected error')
            
    def delete_multi(self, keys, time=0):
        if not keys:
            return
        try:
            with self.clients.reserve() as mc:
                mc.delete_multi(map(key_func, keys))
        except:
            log.exception('unexpected error')
            
    def remove(self):
        pass
    
    def close(self):
        try:
            with self.clients.reserve() as mc:
                mc.disconnect_all()
        except:
            log.exception('unexpected error')
    
    def clear(self):
        try:
            with self.clients.reserve() as mc:
                mc.flush_all()
        except:
            log.exception('unexpected error')
    
    def stat(self):
        pass

cache_proxy = Memcache