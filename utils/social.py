# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

from play.conf import settings

class SocialGraph(object):
    
    def __init__(self, redis, namespace, object_id):
        self.r = redis
        self.ns = namespace
        self.object_id = object_id
        self.follow_key = self.following_key(self.object_id)
    
    def following_key(self,item_id):
        return '%s:%s:%s:follow' % (self.r.namespace, self.ns, item_id)
        
    def follower_key(self,item_id):
        return '%s:%s:%s:follower' % (self.r.namespace, self.ns, item_id)
    
    def follower_rank_key(self):
        return '%s:%s:follower-rank' % (self.r.namespace, self.ns)
    
    def follow(self,item_id):
        self.r.sadd(self.follow_key,item_id)
        self.r.sadd(self.follower_key(item_id),self.object_id)
        self.r.zincrby(self.follower_rank_key(),item_id,amount=1)
    
    def unfollow(self,item_id):
        if self.is_following(item_id):
            self.r.srem(self.follow_key,item_id)
            self.r.srem(self.follower_key(item_id),self.object_id)
            self.r.zincrby(self.follower_rank_key(),item_id,amount=-1)
            
    def is_following(self,item_id):
        return self.r.sismember(self.follow_key,item_id)
    
    def is_followed_by(self,item_id):
        return self.r.sismember(self.follower_key(self.object_id),item_id)
    
    def following(self):
        return map(int,self.r.smembers(self.follow_key))
    
    def followed_by(self):
        return map(int,self.r.smembers(self.follower_key(self.object_id)))
    
    def is_mutual(self,item_id):
        return self.is_following(item_id) and self.is_followed_by(item_id)
    
    def following_count(self):
        return self.r.scard(self.follow_key)
    
    def follower_count(self):
        return self.r.scard(self.follower_key(self.object_id))
    
    def common_following(self,item_ids=None):
        if not item_ids:
            item_ids = self.following()
        if not item_ids:
            return []
        item_ids.append(self.object_id)
        keys = [self.following_key(item_id) for item_id in item_ids]
        return map(int,self.r.sinter(keys))
    
    def common_follower(self,item_ids=None):
        if not item_ids:
            item_ids = self.following()
        if not item_ids:
            return []
        item_ids.append(self.object_id)
        keys = [self.follower_key(item_id) for item_id in item_ids]
        return map(int,self.r.sinter(keys))
    
    def hots(self,limit=10):
        return self.r.zrevrange(self.follower_rank_key(),1,limit,withscores=True,score_cast_func=int)
        
    def suggest(self,limit=10):
        item_ids = self.following()
        item_ids.append(self.object_id)
        keys = [self.following_key(item_id) for item_id in item_ids]
        item_ids = self.r.sdiff(keys)
        return map(int,item_ids)
    
def test_init(r):
    import random
    items = [i for i in xrange(1000,2000)]
    print 'building dataset...'
    users = random.sample(items, 134)
    for id in users:
        sg = SocialGraph(r,'user',id)
        fs = random.sample(items, 69)
        fs = filter(lambda x: x != id, fs)
        for iid in fs:
            sg.follow(iid)
    return users

def test_functions(r,users):
    print 'test functions...'
    for uid in users:
        sg = SocialGraph(r,'user', uid)
        ss = sg.suggest()
        if ss:
            print '-------user-test:', uid
            print 'following:', sg.following()
            print 'follow-by:', sg.followed_by()
            print 'hots:', sg.hots()
            print 'suggest:', sg.suggest()
            print 'common following:', sg.common_following()
            print 'common followers:', sg.common_follower()


if __name__ == '__main__':
    import redis
    r = redis.StrictRedis(host='127.0.0.1',port=63790,db=0)
    items = [i for i in xrange(1000,2000)]
    test_functions(r,items)