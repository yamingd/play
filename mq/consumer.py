# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

from play.mq import qmanager

"""
Queue --> Message(kind) --> Handler
"""
class ConsumerBase(object):
    def __init__(self, backend, qname):
        self.backend = backend
        self.qname = qname
        self.queue = qmanager.find_queue(qname)
        if self.queue is None:
            raise Exception("Please check, can't find Queue with name="+qname)
        
    def start(self):
        self.backend.listen(self)
    
    def on_message(self, data):
        """
        How to handle your message, data is instance ODict
        """
        h = qmanager.find_handler(data)
        h and h.execute(data)
        
    def on_message_fail(self, data):
        pass