# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

from Queue import Queue
from contextlib import contextmanager
from cStringIO import StringIO

from play.utils import jsonfy, ODict
from play.conf import settings

class ConnectionPool(Queue):
    def __init__(self, rc, n_slots=5):
        Queue.__init__(self, n_slots)
        if rc is not None:
            self.fill(rc, n_slots)

    @contextmanager
    def reserve(self, timeout=None):
        """Context manager for reserving a client from the pool.
        If *timeout* is given, it specifiecs how long to wait for a client to
        become available.
        """
        rc = self.get(True, timeout=timeout)
        try:
            yield rc
        finally:
            self.put(rc)

    def fill(self, rc, n_slots):
        """Fill *n_slots* of the pool with clones of *mc*."""
        for i in xrange(n_slots):
            self.put(rc.clone())
            
class EngineBase(object):
    def __init__(self, conf):
        self.conf = conf
    
    def connect(self):
        pass
    
    def close(self):
        pass
    
    def send(self, messages):
        pass
    
    def listen(self, consumer):
        pass
    
    def stop(self):
        pass
    
    def stat(self):
        pass
    
    def __del__(self):
        self.close()
        self.stop()
        
    def on_message_receive(self, content):
        log.debug('[%s] Received: %s' % (self.consumer.qname, content))
        if not content:
            log.error('Content is None')
            return
        #parse message as json
        try:
            content = jsonfy.loads(content)
            if not isinstance(content, dict):
                log.error('cant find handler for this msg %s', content)
                return
            content = ODict(content)
        except:
            log.error('json parse content error: %s',content)
            log.exception('unexpected error:')
            return
        
        #deal with message
        error = False
        try:
            self.consumer.on_message(content)
        except BaseException as e:
            error = True
            log.error('error: %s',content)
            log.exception('unexpected error:')
            self.on_message_failed(e, content)
        finally:
            if error:
                log.error('handle message with error.')
    
    def on_message_failed(self, e, content):
        if settings.site.mail_apperror:
            #mail error
            try:
                s = StringIO()
                traceback.print_exc(file=s)
                content['mq'] = True
                content['subject'] = str(e)
                content['body'] = s.getvalue()
                content['time'] = datetime.now().strftime('%Y%m%d %H:%M:%S')
                s.close()
                self.consumer.on_message_fail(content)
            except:
                log.exception('unexpected error:')