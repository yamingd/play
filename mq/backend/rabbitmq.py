# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import os
from cStringIO import StringIO
from twisted.internet import reactor, defer, protocol
from twisted.internet.protocol import ClientCreator

import amqplib.client_0_8 as amqp

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.message import Message
import txamqp.spec

RECONNECT_DELAY = 5

from play.utils import jsonfy
from play.conf import settings
from play.mq.backend import EngineBase, ConnectionPool

class ConsumerDelegate(TwistedDelegate):
    def __init__(self, disconnected, process):
        TwistedDelegate.__init__(self)
        self._disconnected = disconnected
        self._process = process

    def dispatch(self, ch, msg):
        if msg.method.name == "deliver":
            if self._process:
                self._process(msg)
        else:
            TwistedDelegate.dispatch(self, ch, msg)

    def channel_close(self, ch, msg):
        TwistedDelegate.channel_close(self, ch, msg)
        self._fireDisconnected(msg)

    def connection_close(self, ch, msg):
        TwistedDelegate.connection_close(self, ch, msg)
        self._fireDisconnected(msg)

    def close(self, reason):
        TwistedDelegate.close(self, reason)
        self._fireDisconnected(reason)

    def _fireDisconnected(self, reason):
        if isinstance(reason, Message):
            reason = reason.fields[1]
        if self._disconnected:
            self._disconnected(reason)
        self._disconnected = None
        self._process = None
        
class RabbitMQEngine(EngineBase):
    def __init__(self, conf):
        """
        conf => ('username','passwd','host','port','vhost')
        """
        self.conf = conf
        self.connection = None
        self.count = 0
        self._connstr = "amqp://%s:%s%s/%s" % (self.conf.host, self.conf.port, \
                                               self.conf.vhost, self.queue.exchange)
        
        self.channel = None
        self.cl = None

        self._disconnecting = False
        self._reconnect_delay = None
        self.spec_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),u'amqp0-8.xml')
        log.info('init RabbitMQEngine')
        
    def connect(self):
        if not self.connection:
            self.connection = amqp.Connection(self.conf.host,
                        userid=self.conf.username,
                        password=self.conf.passwd,
                        virtual_host=self.conf.vhost,
                        ssl=False)
            self.pool = ConnectionPool(None, self.conf.pools)
            for c in xrange(self.conf.pools):
                self.pool.put(self.connection.channel(c+1))
            
    def close(self):
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
            except:
                log.exception('unexpected error')
    
    def send(self, messages):
        self.connect()
        if not self.connection:
            raise Exception("RabbitMQ can't connect to server.")
        if not isinstance(messages, list):
            messages = list(messages)
        total = 0
        with self.pool.reserve() as c:
            for message in messages:
                try:
                    exchange_name = message.queue.exchange
                    routing_key = message.queue.routing_key
                    msg = amqp.Message(jsonfy.dumps(message))
                    msg.properties["delivery_mode"] = 2 # persistant
                    c.basic_publish(msg, exchange=exchange_name, routing_key=routing_key)
                    total = total + 1
                except:
                    log.exception('unexpected error')
        return total
    
    def listen(self, consumer):
        self._spec = txamqp.spec.load(self.spec_path)
        self.consumer = consumer
        self.queue = consumer.queue
        def gotConnection(conn):
            log.debug("Authenticating user %s..." % self.conf.username)
            d = conn.authenticate(self.conf.username, self.conf.passwd)
            d.addCallback(connectionAuthenticated, conn)
            return d

        def connectionAuthenticated(_, conn):
            log.debug("Allocating AMQP channel...")
            return conn.channel(self.queue.chan).addCallback(gotChannel, conn)

        def gotChannel(chan, conn):
            log.debug("Opening AMQP channel...")
            return chan.channel_open().addCallback(declareQueue, chan, conn)

        def declareQueue(_, chan, conn):
            log.debug("Declaring Queue %s..." % self.queue.name)
            d = chan.queue_declare(queue=self.queue.name, durable=True,
                                   exclusive=False, auto_delete=False)
            return d.addCallback(declareExchange, chan, conn)

        def declareExchange(_, chan, conn):
            log.debug("Declaring Exchange %s..." % self.queue.exchange)
            d = chan.exchange_declare(exchange=self.queue.exchange, durable=True,
                                      type="direct", auto_delete=False)
            return d.addCallback(bindQueue, chan, conn)

        def bindQueue(_, chan, conn):
            log.debug("Binding queue %s with exchange %s on key pattern %s..." % (self.queue.name, self.queue.exchange, self.queue.routing_key))
            d = chan.queue_bind(queue=self.queue.name, exchange=self.queue.exchange,
                                routing_key=self.queue.routing_key)
            return d.addCallback(startConsumer, chan, conn)

        def startConsumer(queue, chan, conn):
            log.debug("Start consuming messages...")
            d = chan.basic_consume(queue=self.queue.name, no_ack=True,
                                   consumer_tag=self.queue.consumer_tag)
            return d.addCallback(started, chan, conn)

        def started(_, chan, conn):
            log.debug("Connected to %s" % self._connstr)
            self.connection = conn
            self.channel = chan

        def gotDisconnected(reason):
            log.info("Disconnected:")
            log.info(reason)
            reconnect()

        def connectionFailed(failure):
            log.error("Connection Failed:")
            log.error(failure.getErrorMessage())
            reconnect()

        def reconnect():
            log.debug("Reconnecting in %s seconds" % RECONNECT_DELAY)
            if not self._disconnecting:
                dc = reactor.callLater(RECONNECT_DELAY, connect)
                self._reconnect_delay = dc

        def connect():
            delegate = ConsumerDelegate(gotDisconnected, self._processMessage)
            cli = protocol.ClientCreator(reactor, AMQClient, delegate=delegate,
                                         vhost=self.conf.vhost, spec=self._spec)
            log.debug("Connecting to %s:%s..." % (self.conf.host, self.conf.port))
            d = cli.connectTCP(self.conf.host, self.conf.port)

            return d.addCallbacks(gotConnection, connectionFailed)

        print "Starting AMQP consumer for %s" % self._connstr
        return connect()
    
    def stop(self):
        def cancelConsumer(_, channel):
            log.debug("Stop consuming %s..." % self._constag)
            return channel.basic_cancel(self.queue.consumer_tag)

        def closeChannel(_, channel):
            log.debug("Closing channel...")
            return channel.channel_close()

        def getNewChannel(_, connection):
            log.debug("Getting a new channel for closing connection...")
            return connection.channel(0)

        def closeConnection(channel):
            log.debug("Closing connection...")
            return channel.connection_close()

        def connectorStopped(_):
            log.debug("Connector stopped")
            return self

        log.debug("Stopping connector to %s" % self._connstr)
        d = defer.succeed(None)

        self._disconnecting = True
        if self._reconnect_delay:
            self._reconnect_delay.cancel()
            self._reconnect_delay = None

        if self._dc is not None:
            self._dc.cancel()
            d.addCallback(cancelConsumer, self.channel)
            self._dc = None

        if self.channel is not None:
            d.addCallback(closeChannel, self.channel)
            self.channel = None

        if self.connection is not None:
            d.addCallback(getNewChannel, self.connection)
            d.addCallback(closeConnection)
            self.connection = None

        return d.addCallback(connectorStopped)
    
    def stat(self):
        pass
    
    def _processMessage(self, msg):
        content = msg.content.body
        self.on_message_receive(content)

mq_proxy = RabbitMQEngine