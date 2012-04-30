# -*- coding: utf-8 -*-

def chinese_key(term):
    term2 = repr(term)
    if '\\u' in term2:
        term2 = term2.replace('\\u','').replace('\'','')
        return term2[1:]
    return term

class ACBackend(object):
    def __init__(self,r,pinyin=None,namespace=None):
        pass
    
    def put(self,title,item_id):
        pass
    
    def remove(self,title,item_id):
        pass
    
    def suggest(self,phrase,start=1,limit=10,namespace='',expires=600):
        pass
    