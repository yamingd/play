# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import random
import re
import os
import urllib,hashlib,time,urlparse
from lxml.html import fromstring
from cStringIO import StringIO
import pycurl

user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 Firefox/3.5.7 (.NET CLR 3.5.30729)'

default_headers = {
    'Accept': 'text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
    'Accept-Language': 'ru,en-us;q=0.%(x)d,en;q=0.3;%(lang)s' % {'x': random.randint(5, 9),
                                                               'lang': random.choice(['ua', 'gb', 'uk'])},
    'Accept-Charset': 'utf-8,gbk,gb2312,windows-1251;q=0.%(x)d,*;q=0.%(x)d' % {'x': random.randint(5, 9)}
}

img_type_map = {'image/jpeg':'.jpg','image/gif':'.gif','image/x-png':'.png'}

cookie_file = os.path.join(os.environ.get("HOME"),'.d9x_cookie.txt')

def page_charset(curl):
    content_type = curl.getinfo(pycurl.CONTENT_TYPE)
    if content_type is not None:
        s = re.findall('charset=(.+)',content_type.lower())
        if s:
            return s[0]
    return None

def urldomain(url):
    met = urlparse.urlsplit(url)
    return met.netloc

def _open(url,fp,timeout=5):
    curl = pycurl.Curl()
    curl.setopt(pycurl.URL, str(url.encode('utf8')))
    curl.setopt(pycurl.WRITEFUNCTION, fp.write)
    curl.setopt(pycurl.FOLLOWLOCATION, 1)
    #curl.setopt(pycurl.MAXREDIRS, 1)
    curl.setopt(pycurl.OPT_FILETIME, 1)
    if timeout:
        curl.setopt(pycurl.CONNECTTIMEOUT, timeout)
        #curl.setopt(pycurl.TIMEOUT, timeout)
    curl.setopt(pycurl.USERAGENT, user_agent)
    curl.setopt(pycurl.HTTPHEADER, ['%s: %s' % (a, b) for a, b in default_headers.iteritems()])
    curl.setopt(pycurl.COOKIEFILE, cookie_file)
    curl.setopt(pycurl.COOKIEJAR, cookie_file)
    curl.perform()
    return curl

def _getcontent(url,timeout=5):
    html = StringIO()
    try:
        log.debug('fetch content:%s'% url)
        curl = _open(url,html,timeout=timeout)
        data=html.getvalue()
        code = page_charset(curl)
        curl.close()
        return code,data
    except:
        log.exception('unexpected error:%s' % url)
        raise
    finally:
        html.close()
        
def get_image(url,fp,timeout=5):
    try:
        log.debug('fetch img:%s'% url)
        curl = _open(url,fp,timeout=timeout)
        #通过content-type推动图片后缀
        content_type = curl.getinfo(curl.CONTENT_TYPE)
        curl.close()
        if content_type == u'text/html':
            log.error('return %s' % content_type)
            raise Exception(content_type)
        img_type = img_type_map.get(content_type,'.jpg') if content_type else '.jpg'
        return img_type
    except: 
        log.exception('unexpected error:%s',url)
        raise
    
def get_htmldoc(url,timeout=5):
    code,data = _getcontent(url,timeout=timeout)
    encode = 'utf8'
    if code:
        encode = code
    try:
        codedata = data
        try:
            codedata = unicode(data,encode)
        except:
            try:
                codedata = unicode(data,'GB18030')
            except:
                try:
                    codedata = unicode(data,'utf8')
                except:
                    pass
        doc = fromstring(codedata)
        #log.debug(codedata)
        return doc
    except: 
        log.exception('unexpected error:%s(%s)' % (url,code))
        raise
    
def get_xmldoc(url,timeout=5):
    code,data = _getcontent(url,timeout=timeout)
    doc = fromstring(data)
    return doc