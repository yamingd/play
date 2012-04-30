# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import mmseg
import re

from play import app_global
from play.suggest import ACBackend, chinese_key
from play.utils import pinyinfy

MIN_WORDS = 2
MAX_WORDS = 3
SUFFIX_MIN_LENGTH = 1
    
class RedisAC(ACBackend):
    phrase_key_prefix = 'ac-phrase'
    suffix_key_prefix = 'ac-suffix'
    
    def __init__(self, namespace=None):
        self.terminator = '^'
        self.namespace = ':' + namespace + ':' if namespace else ':'
    
    @property
    def pinyin(self):
        if not hasattr(self, '_pinyin'):
            self._pinyin = pinyinfy.Pinyin()
            self._pinyin.load()
        return self._pinyin
    
    @property
    def r(self):
        return app_global.redis
    
    def _add_phrase(self,phrase,item_id):
        phrase = re.sub(u'\\s+','',phrase)
        key = self.phrase_key_prefix + self.namespace + phrase.lower()
        self.r.zadd(key,1,item_id)
        
    def _rem_phrase(self,phrase,item_id):
        phrase = re.sub(u'\\s+','',phrase)
        key = self.phrase_key_prefix + self.namespace + phrase.lower()
        self.r.zrem(key,item_id)
        return self.r.zcard(key) == 0
    
    def _add_suffix(self,key,value,score):
        key = self.suffix_key_prefix + self.namespace + key.lower()
        self.r.zadd(key,score,value.lower())
        
    def _remove_suffix(self,phrase):
        pass
    
    def _gen_suffix(self,phrase):
        terms = phrase
        key = terms[0:SUFFIX_MIN_LENGTH]
        for c in terms[SUFFIX_MIN_LENGTH:]:
            yield (chinese_key(key), c, ord(c))
            key = key + c
        yield (chinese_key(phrase), self.terminator, 0)

    def _gen_pinyin_phrase(self,words):
        """
        Break apart a phrase into several chunks using max_words as a guide
        The quick brown fox jumped --> quick brown fox, brown fox jumped
        """
        max_words = max(
            min(len(words), MAX_WORDS), MIN_WORDS
        )
        
        for num_words in range(MIN_WORDS, max_words + 1):
            chunks = len(words) - num_words + 1
            chunks = chunks < 1 and 1 or chunks
            
            for i in range(chunks):
                yield ' '.join(words[i:i + num_words])
    
    def put(self,title,item_id):
        """
        title --> segment --> sadd(phrase,item_id) -> zadd (phrase->prefix, suffix, 0)
                          --> pinyin --> sadd(phrase,item_id) -> zadd (phrase->prefix, suffix, 0)
        """
        if not title or not item_id:
            return
        
        for phrase in mmseg.seg_txt(title.encode('utf8')):
            if not phrase:
                continue
            phrase = phrase.decode('utf8')
            self._add_phrase(chinese_key(phrase),item_id)
            for (key,suffix,score) in self._gen_suffix(phrase):
                self._add_suffix(key,chinese_key(suffix),score)
            
            if not self.pinyin:
                continue
            
            phrase = self.pinyin.translate(phrase)
            if not phrase:
                continue
            
            for sub_phrase in self._gen_pinyin_phrase(phrase):
                self._add_phrase(sub_phrase,item_id)
                for (key,suffix,score) in self._gen_suffix(re.sub('\\s+','',sub_phrase)):
                    self._add_suffix(key,suffix,score)
            
    def remove(self,title,item_id):
        if not title or not item_id:
            return
        
        for phrase in mmseg.seg_txt(title.encode('utf8')):
            if not phrase:
                continue
            
            phrase = phrase.decode('utf8')
            self._rem_phrase(chinese_key(phrase),item_id)

            if not self.pinyin:
                continue
            
            phrase = self.pinyin.translate(phrase)
            if not phrase:
                continue
            
            for sub_phrase in self._gen_pinyin_phrase(phrase):
                self._rem_phrase(sub_phrase,item_id)
    
    def suggest(self,phrase,start=1,limit=10,namespace='',expires=600):
        temp = re.split('\s+',phrase.strip())
        phrase = [item for item in mmseg.seg_txt(phrase.encode('utf8'))]
        phrase.extend(temp)
        phrase = map(chinese_key,phrase)
        start = (start-1)*limit
        result_key = 'ac-suggest:' + '|'.join(phrase)
        results = self.r.zrevrange(result_key,start,start+limit-1)
        if results:
            return results
        
        prefix = self.suffix_key_prefix + self.namespace
        prefix_len = len(prefix)
        phrase_keys = []
        for sub_phrase in phrase:
            key =  prefix + sub_phrase
            results = self._suggest(key, limit)
            # strip the prefix off the keys that indicated they matched a lookup
            cleaned_keys = map(lambda x: x[prefix_len:], results)
            cleaned_keys = map(lambda x: self.phrase_key_prefix + self.namespace+x, cleaned_keys)
            phrase_keys.extend(cleaned_keys)
        
        if not phrase_keys:
            return []
        #union all
        num = self.r.zinterstore(result_key,list(set(phrase_keys)))
        self.r.expire(result_key,expires)
        #results
        results = self.r.zrevrange(result_key,start,start+limit-1)
        return results
    
    def _suggest(self, text, limit):
        """
        At the expense of key memory, depth-first search all results
        """
        w = []
        for char in self.r.zrange(text, 0, -1):
            if char == self.terminator:
                w.append(text)
            else:
                w.extend(self._suggest(text + char, limit))
            if limit and len(w) >= limit:
                return w[:limit]
        return w

def test_ac():
    import redis
    r = redis.StrictRedis(host='127.0.0.1',port=63790,db=1)
    lookup = pinyinfy.Pinyin(r)
    
    r = redis.StrictRedis(host='127.0.0.1',port=63790,db=2)
    ac = RedisAC(r,lookup,namespace='song')
    
    #title = u'李克勤-婚前的女人'
    #ac.put(title,1000)
    
    #title = u'李克勤-月半小夜曲'
    #ac.put(title,1001)
    
    #title = u'李克勤-总有你鼓励'
    #ac.put(title,1002)
    
    title = u'How much are each of these U.S. Presidents responsible for the attributed national debt?'
    ac.put(title,1003)
    
    print ac.suggest(u'how')
    print ac.suggest(u'女')
    print ac.suggest(u'李 女')
    
if __name__ == '__main__':
    test_ac()
    