# -*- coding: utf-8 -*-
import os,sys

from play.utils import ODict

apps = []

#Site Configuration
site = {
    "cookie_secret": "61oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
    "login_url": "/account/index",
    "gzip" : False,
    'logging':'debug',
    "title": u"_site_title",
    "keywords": u"_site_keyworks_",
    "description": u"_site_description_",
    "domain": u"_site_domain",
    "cdn": u"_site_cdn_url",
    "feedback": u"_site_feedback_mail",
    "webmaster": u"_site_webmaster_mail",
    "admin_mail": u"_site_admin_mail",
    "runtime": u"development",
    "open_signup":False,
    "google_analytics":False,
    "mail_apperror":False,
    "optimize_static_content":False,
    "debug":True,
    "mail_error":True
}
site = ODict(site)

#SMTP Configuration
mail = {
    "engine":"play.mail.backends.smtp", #console, locmem, smtp
    "host": u"localhost",
    "port": 25,
    "user": u"",
    "passwd": u"",
    "use_tls": False,
    "fail_silently": False
}
mail = ODict(mail)

#Session Security
session = {
    "key": u"quora",
    "secret": site.cookie_secret,
    "cookie_expires": True,
    "cookie_expires_days": 30,
    "cookie_id":"_sess",
    "auth_cookie":"_auth"
}
session = ODict(session)

#cache
cache = {
    "engine": "play.cache.memcache",
    "servers": ['127.0.0.1:11219'],
    "pools": 5,
    "namespace": "quora",
}
cache = ODict(cache)

#Redis
redis = {
    'engine':'play.nosql.shard_redis', #'play.nosql.redis0'
    'mod' : 128,
    'namespace': 'quora',
    'nodes' : {
        'redis_1' : '192.168.1.100:63790',
        'redis_2' : '192.168.1.100:63791',
        'redis_3' : '192.168.1.101:63790',
        'redis_4' : '192.168.1.101:63791'
    }
}
redis = ODict(redis)

#DataEngine
dbm = {
    "engine": "play.db.mysql",
    "host": u"127.0.0.1:3309",
    "user": u"quora",
    "passwd": u"quora-dev",
    "dbname": u"quora_dev",
    "pools": 5,
    "debug": site.debug,
}
dbm = ODict(dbm)

#MQ
mq = {
    'engine': 'play.mq.backend.beanstalk', #'play.mq.backend.rabbitmq/beanstalk'
    "host": u"127.0.0.1",
    "port": 11300,
    "pools": 5,
    "username": "test",
    "passwd": "test",
    "vhost": "/test",
    "exchange": "test_exchange"
}
mq = ODict(mq)

#Static - Images
image = {
    'engine':'play.image.localfs', #'localfs/mogilefs'
    "trackers": [u"127.0.0.1:7001"],
    "domain": u"quora",
    "pools": 5,
    "root_folder":""
}
image = ODict(image)

#Search Engine
search = {
    "engine":"play.search.solr",
    "url": u"http://127.0.0.1:8393/solr",
    "pools": 5,
    "cores": [],
    "data":"/var/solr/data"
}
search = ODict(search)

#Others
spiders = ["Googlebot","Baiduspider","Yahoo! Slurp","YodaoBot","msnbot","Googlebot-Mobile", \
           "Sosospider","Sogou web spider"]

#Customs Settings
sina_consumer_key = "sina_consumer_key"
sina_consumer_secret = "sina_consumer_secret"
