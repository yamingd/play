# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import time

def log_query(func):
    """
    A decorator for pseudo-logging search queries. Used in the ``SearchBackend``
    to wrap the ``search`` method.
    """
    def wrapper(obj, query_string, *args, **kwargs):
        start = time.time()
        try:
            return func(obj, query_string, *args, **kwargs)
        finally:
            stop = time.time()
            log.info('query %s, duration=%s seconds', query_string, stop-start)
    return wrapper

class BaseSearchBackend(object):
    def __init__(self, conf):
        self.conf = conf
        self.engine = None
        
    def add(self, docs):
        pass
    
    def update(self, docs):
        pass
    
    def search(self, q, page=1, size=10, sorts=[], fields=[], facet=None, facet_fields=[], wrapper=None):
        """
        return ResultSet, FacetSet
        """
        pass
    
    def more_like_this(self, q, size=10, fields=[], wrapper=None):
        """
        return ResultSet
        """
        pass
    
    def term_suggest(self, q, prefix, fields=[], limit=10, mincount=1):
        pass
    
    
    def clear(self):
        pass
    
    def rebuild(self):
        pass
    
    def optimize(self):
        pass

class FacetSet(object):
    def __init__(self, set):
        self.ori_set = set
        self.fields = set['facet_fields'] if set else {}
    
    def __getattr__(self, key):
        try:
            return self.fields[key]
        except KeyError, k:
            return None
    
    def __iter__(self):
        for key in self.fields:
            yield key, self.fields[key]
    
class ResultSet(object):
    def __init__(self, items, wrapper=None, total=0, page=1, size=0):
        self.items = map(int, items)
        self.total = total
        self.wrapper = wrapper
        self.size = size
        self.current_page = page
        self._caches = {}
    
    @property
    def pages(self):
        ps = 0
        if self.size>0:
            ps = self.total / self.size
            if self.total % self.size > 0:
                ps = ps + 1
        return ps
    
    def filter(self, ids):
        if ids:
            self.items = list(set(self.items) - set(ids))
    
    def __len__(self):
        return len(self.items)
    
    def _litem(self, wid):
        if wid in self._caches:
            return self._caches[wid]
        self._caches[wid] = ret = self.wrapper(wid)
        return ret
    
    def __getitem__(self, index):
        if isinstance(index,int):
            wid = self.items[index]
            return self._litem(wid)
        elif isinstance(index,slice):
            wids = self.items[index]
            rets = []
            for wid in wids:
                rets.append(self._litem(wid))
            return rets
        
    def __iter__(self):
        for i in xrange(len(self)):
            yield self[i]
    
    def __repr__(self):
        return '<ResultSet (%s)>' % self.items
    
    def pop(self):
        if self.items:
            wid = self.items.pop()
            return self._litem(wid)
        return None