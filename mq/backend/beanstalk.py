# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

from cStringIO import StringIO
from twisted.internet import reactor, protocol, defer, task

import beanstalk

from play.utils import jsonfy
from play.conf import settings
from play.mq.backend import EngineBase, ConnectionPool

class BeanstalkEngine(EngineBase):
    def __init__(self, conf):
        """
        conf ==> ('host','port')
        """
        self.conf = conf
        self.conn = None
        self.count = 0
        self.pool = None
        log.info('init BeanstalkEngine')
        
    def connect(self):
        if not self.pool:
            conn = beanstalk.serverconn.ServerConn(self.conf.host, self.conf.port)
            self.pool = ConnectionPool(conn, n_slots=self.conf.pools)
            log.info('BeanstalkEngine connected')
    
    def close(self):
        if self.pool:
            with self.pool.reserve() as conn:
                conn.close()
    
    def send(self, messages):
        self.connect()
        if not self.pool:
            raise Exception('can not connect beanstalk server.')
        if not isinstance(messages, list):
            messages = list(messages)
        total = 0
        with self.pool.reserve() as conn:
            for message in messages:
                try:
                    ttr = message.get('ttr', 60)
                    conn.use(message.queue.name)
                    conn.put(jsonfy.dumps(message), ttr=int(ttr))
                    self.count = self.count + 1
                    total = total + 1
                    log.debug('mq message count:%s' % self.count)
                except:
                    log.exception('unexpected error')
        return total
    
    def stat(self):
        pass
        
    def listen(self, consumer):
        self.consumer = consumer
        self.queue = consumer.queue
        def _executor(bs, msg):
            bs.touch(msg['jid'])
            content = msg['data']
            self.on_message_receive(content)
            try:
                bs.delete(jobdata['jid'])
            except:
                log.exception('unexpected error:')

        def _error_handler(e):
            log.error("Got an error:%s", e)

        def _executionGenerator(bs):
            while True:
                yield bs.reserve()\
                    .addCallback(lambda v: _executor(bs, v))\
                        .addErrback(_error_handler)

        def _worker(bs):
            bs.watch(self.consumer.qname)
            bs.ignore("default")
            
            log.info('Beanstalk Engine Start watching:%s' % self.queue.name)
            coop = task.Cooperator()
            coop.coiterate(_executionGenerator(bs))
            
        def _connect():
            d=protocol.ClientCreator(reactor,
                beanstalk.twisted_client.Beanstalk).connectTCP(self.conf.host, self.conf.port)
            return d.addCallback(_worker)
        
        return _connect()

mq_proxy = BeanstalkEngine