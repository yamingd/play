# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

from play import app_global

import re
from datetime import datetime

class Pinyin(object):
    namespace = 'Pinyin'
    def __init__(self):
        pass
    
    @property
    def r(self):
        return app_global.redis
    
    def is_loaded(self):
        s = self.r.get(self.namespace + ':v')
        return not not s
    
    def load(self):
        if self.is_loaded():
            return
        self.reload()
        
    def reload(self):
        with open('uc-to-py.tbl','r') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                self._put_line(line)
        
        self.r.set(self.namespace + ':v', str(datetime.now()))
    
    def _put_line(self,line):
        temp = re.split('\s+',line)
        key = temp[0]
        temp = temp[1].split(',')
        temp = map(lambda x: re.sub('[0-9:\(\)]','',x), temp)
        temp = set(temp)
        
        self.r.set(self.namespace + ':' + key.lower(), ','.join(temp))
    
    def add(self,key,values):
        key = repr(key)[4:-1]
        self.r.set(self.namespace + ':' + key.lower(), values)
    
    def _find(self,ucode):
        ss = self.r.get(self.namespace + ':' + ucode)
        return ss.split(',') if ss else []
        
    def translate(self,word):
        rts = []
        for c in word:
            cc = repr(c)
            if cc.startswith('u\'\\'):
                rts.extend(self._find(cc[4:-1]))
            else:
                rts.append(c)
        return rts
    
    def dumps(self):
        keys = self.r.keys(self.namespace + ':*')
        with open('pinyin.txt','w+') as f:
            for key in keys:
                values = self.r.get(key)
                key = key.split(':')[-1]
                if key != 'v':
                    f.write('%s (%s)' % (key, values))
                    f.write('\n')
    
if __name__ == '__main__':
    import redis
    r = redis.StrictRedis(host='127.0.0.1',port=63790,db=1)
    lookup = Pinyin(r)
    print lookup.translate(u'中国')
    print lookup.translate(u'穷我一生')
    lookup.dumps()