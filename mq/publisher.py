# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

class PublisherBase(object):
    def __init__(self, backend):
        self.count = 0
        self.backend = backend
    
    def send(self, msgs):
        try:
            total = self.backend.send(msgs)
            self.count = self.count + total
            log.debug('send message count:%s' % self.count)
        except:
            log.exception('unexpected error')