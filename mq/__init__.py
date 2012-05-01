# -*- coding: utf-8 -*-

__all__ = ['QueueManager','qmanager','queue_declare','Message','MessageHandler','handler']

import time
from datetime import datetime

from play.utils import ODict
from play.conf import settings
from play import app_global

class MQueue(object):
    def __init__(self,name):
        self.chan = 1
        self.name = name
        self.exchange = settings.mq.exchange
        self.routing_key = name + '_routing_key'
        self.consumer_tag = name + '_consumer_tag'

class QueueManager(object):
    def __init__(self):
        self.queues = {}
        self.mqueues = {}
        self.handlers = {}
    
    def declare(self, kind, qname):
        mq = MQueue(qname)
        mq.chan = len(self.queues)+10
        self.queues[qname] = mq
        self.mqueues[kind] = mq
        return mq
    
    def find_queue(self,name):
        return self.queues.get(name)
    
    def find_msg_queue(self,kind):
        return self.mqueues.get(kind)
    
    def add_handler(self, opid, handler):
        self.handlers[opid] = handler
    
    def find_handler(self, message):
        return self.handlers.get(message.opid)
        
qmanager = QueueManager()

def queue_declare(kind, qname):
    """
    @queue_declare('user','quser', fake_handlers)
    class UserMessage(Message): pass
    """
    def _wrap(clz):
        clz.kind = kind
        clz.queue = qmanager.declare(kind, qname)
        return clz
    return _wrap

class Message(ODict):
    def __init__(self, user, opid, **kwargs):
        """
        use opid to find MessageHandler
        """
        kwargs['kind'] = self.__class__.kind
        kwargs['ts'] = datetime.now().strftime('%Y%m%d %H:%M:%S')
        if user:
            kwargs['user_id'] = user.id
            kwargs['user_ip'] = getattr(user, 'client_ip', '')
        kwargs['opid'] = opid
        ODict.__init__(**kwargs)

def handler(opid):
    def _wrap(clz):
        clz.opid = opid
        qmanager.add_handler(opid, clz)
        return clz
    return _wrap

class MessageHandler(object):
    opid = None
    
    @classmethod
    def execute(clz, message):
        pass
    