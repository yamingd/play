# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

from lxml.html import fromstring

"""
找出如下超链接
<a rel='Question' relid='122'>title</a>
<a rel='Topic' relid='123'>topic</a>
<a rel='Answer' relid='234'>answer</a>
<a rel='User' relid='678'>person</a>
<a rel='Post' relid='444'>post title</a>
"""
def find_objects(text):
    from quora.model import EntityCatalog
    
    doc = fromstring(text)
    links = doc.cssselect('a')
    result = {}
    for link in links:
        rel = link.get('rel')
        relid = link.get('relid')
        href = link.get('href')
        if rel and relid:
            result.setdefault(rel,[])
            if rel=='url':
                result[rel].append(href)
            else:
                result[rel].append(int(relid))
    
    sets = {}
    for k in result:
        kindid = getattr(EntityCatalog,k)
        sets[kindid] = sorted(list(set(result[k])))
    return sets

def diff(new_sets,old_sets):
    if not old_sets:
        return new_sets,None
    result0 = {}
    result1 = {}
    
    all_keys = []
    all_keys.extend(new_sets.keys())
    all_keys.extend(old_sets.keys())
    all_keys = list(set(all_keys))
    
    for k in all_keys:
        result1[k] = set(new_sets[k])-set(old_sets[k])
        result0[k] = set(old_sets[k])-set(new_sets[k])
    
    return result1,result0