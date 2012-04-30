# -*- coding: utf-8 -*-
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
    
    def declare(self, kind, qname, handlers):
        mq = MQueue(qname)
        mq.chan = len(self.queues)+10
        self.queues[qname] = mq
        self.mqueues[kind] = mq
        self.handlers[kind] = handlers
        return mq
    
    def find_queue(self,name):
        return self.queues.get(name)
    
    def find_msg_queue(self,kind):
        return self.mqueues.get(kind)
    
    def find_handler(self, message):
        kind = message.kind
        opid = message.opid
        _h = self.handlers[kind]
        #find model
        m = app_global.models.find(opid/10)
        #find op-name
        opname = m.ops.name(opid)
        if opname:
            _f = getattr(_h,'op_' + opname)
            return _f
        return None
        
qmanager = QueueManager()

def queue_declare(kind, qname, handlers):
    """
    @queue_declare('user','quser', fake_handlers)
    class UserMessage(Message): pass
    """
    def _wrap(clz):
        clz.kind = kind
        clz.queue = qmanager.declare(kind, qname, handlers)
        return clz
    return _wrap

class Message(ODict):
    def __init__(self, user, opid, **kwargs):
        kwargs['kind'] = self.__class__.kind
        kwargs['ts'] = datetime.now().strftime('%Y%m%d %H:%M:%S')
        if user:
            kwargs['user_id'] = user.id
            kwargs['user_ip'] = getattr(user, 'client_ip', '')
        kwargs['opid'] = opid
        ODict.__init__(**kwargs)