# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import binascii
import uuid
import traceback,sys,os
import urlparse
import urllib

from pprint import pprint
from datetime import datetime
from cStringIO import StringIO

import tornado.web

from play.conf import settings
from play.utils import ODict, jsonfy, agentparser
from play.web import helper
from play.mail import send_email
from play import app_global

class PlayRequestHandler(tornado.web.RequestHandler):
    SUPPORTED_METHODS = ("GET", "POST")
    requires_auth_action = []
    requires_auth = False
    
    def __init__(self, application, request, transforms=None):  
        tornado.web.RequestHandler.__init__(self, application, request) 
        self.c = ODict()
        self.g = app_global
        self.models = app_global.models
        self.conf = settings
        self.h = helper
            
    @property
    def user_agent(self):
        """
        {'os': {'name': 'Linux'},'browser': {'version': '5.0.307.11', 'name': 'Chrome'}}
        """
        ss = self.request.headers['User-Agent']
        if ss:       
            return agentparser.detect(ss)
        return None
    
    @property
    def referer(self):
        return self.request.headers.get('Referer','')
        
    @property
    def is_spider(self):
        agent = self.user_agent
        if not agent:
            return False
        for m in self.conf.spiders:
            if m in agent:
                return True
        return False
    
    @property
    def is_xhr(self):
        h = self.request.headers.get('X-Requested-With', None)
        return h and h == 'XMLHttpRequest'
    
    @property
    def client_ip(self):
        return self.request.remote_ip
    
    @property
    def has_session(self):
        sessid = self.get_cookie(self.conf.session.cookie_id)
        return not not sessid
    
    @property
    def session_id(self):
        sessid = self.get_cookie(self.conf.session.cookie_id)
        if not sessid:
            sessid = binascii.b2a_hex(uuid.uuid4().bytes)
            self.set_cookie(self.conf.session.cookie_id, sessid, 
                            expires_days=self.conf.session.cookie_expires_days)
        return sessid
            
    def prepare(self):
        """Called at the beginning of a request before `get`/`post`/etc.

        Override this method to perform common initialization regardless
        of the request method.
        """
        if not self.has_session:
            #尝试通过Cookie登录,产生SessionId
            user = self.get_current_user()
            sessid = self.session_id
            if user:
                user.session_id = sessid
                user.client_ip = self.client_ip
                self.on_cookie_signin()
                
        if self.current_user:
            self.current_user.session_id = self.session_id
            self.current_user.client_ip = self.client_ip
        
        #authorization checking
        if not self.current_user and self.requires_auth:
            if self.is_xhr:
                msg = self.__class__.__name__
                raise tornado.web.HTTPError(403, msg)
            else:
                if self.request.method in ("GET", "HEAD"):
                    url = self.get_login_url()
                    if "?" not in url:
                        if urlparse.urlsplit(url).scheme:
                            # if login url is absolute, make next absolute too
                            next_url = self.request.full_url()
                        else:
                            next_url = self.request.uri
                        url += "?" + urllib.urlencode(dict(next=next_url))
                    self.redirect(url)
                    return
                raise tornado.web.HTTPError(403, self.__class__.__name__)
                
    def on_cookie_signin(self):
        """
        this method will be execute on user signin with cookies
        """
        pass
    
    def on_finish(self):
        """Called after the end of a request.

        Override this method to perform cleanup, logging, etc.
        This method is a counterpart to `prepare`.  ``on_finish`` may
        not produce any output, as it is called after the response
        has been sent to the client.
        """
        pass
    
    def get_current_user(self):
        """Override to determine the current user from, e.g., a cookie."""
        if not hasattr(self,'_current_user'):
            cookie_id = str(self.conf.session.auth_cookie)
            userid = self.get_secure_cookie(cookie_id) 
            if not userid:
                self._current_user = None
            else:
                self._current_user = self.models["User"].find_byid(int(userid))   
        return self._current_user
    
    def remember_user(self, user, remember_me):
        cookie_id = str(self.conf.session.auth_cookie)
        if user:
            self._current_user = user
            if remember_me:
                self.set_secure_cookie(cookie_id,str(user.id),expires_days=self.conf.session.cookie_expires_days)
            else:
                self.set_secure_cookie(cookie_id,str(user.id),expires_days=None)
        else:
            self.set_secure_cookie(cookie_id,str(-1),expires_days=None)
    
    def clear_user(self):
        cookie_id = str(self.conf.session.auth_cookie)
        self.clear_cookie(cookie_id)
    
    def abort(self, code, msg='abort'):
        raise tornado.web.HTTPError(code, msg)
    
    def write_json(self, ct, javascript=False):
        self.set_header("Content-Type", "text/javascript; charset=UTF-8")
        self.write(jsonfy.dumps(ct))
    
    def get_cdn_prefix(self):
        """return something that can be put in front of the static filename
        E.g. if filename is '/static/image.png' and you return '//cloudfront.com'
        then final URL presented in the template becomes
        '//cloudfront.com/static/image.png'
        """
        return self.application.settings.get('cdn_prefix')
        
    def _execute(self, transforms, *args, **kwargs):
        """
        重写,发送错误邮件
        """
        try:
            tornado.web.RequestHandler._execute(self, transforms, *args, **kwargs)
        except Exception as e:
            if not isinstance(e, tornado.web.HTTPError):
                if self.conf.site.mail_error:
                    self._email_exception(e)
                log.exception('unexpected error')
            raise e
    
    def _request_summary(self):
        return '%s uri=%s referer=%s (%s)' % (self.request.method,
                                              self.request.uri,
                                              self.referer,
                                              self.request.remote_ip)
                
    def _email_exception(self, exception): # pragma: no cover
        import sys
        err_type, err_val, err_traceback = sys.exc_info()
        error = u'%s: %s' % (err_type, err_val)
        out = StringIO()
        subject = "%r on %s" % (err_val, self.request.path)
        print >> out, "TRACEBACK:"
        traceback.print_exception(err_type, err_val, err_traceback, 500, out)
        traceback_formatted = out.getvalue()
        print traceback_formatted
        print >> out, "\nREQUEST ARGUMENTS:"
        arguments = self.request.arguments
        if arguments.get('password') and arguments['password'][0]:
            password = arguments['password'][0]
            arguments['password'] = password[:2] + '*' * (len(password) - 2)
        pprint(arguments, out)

        print >> out, "\nCOOKIES:"
        for cookie in self.cookies:
            print >> out, "  %s:" % cookie,
            print >> out, repr(self.get_secure_cookie(cookie))

        print >> out, "\nREQUEST:"
        for key in ('full_url', 'protocol', 'query', 'remote_ip',
                    'request_time', 'uri', 'version'):
            print >> out, "  %s:" % key,
            value = getattr(self.request, key)
            if callable(value):
                try:
                    value = value()
                except:
                    pass
            print >> out, repr(value)

        print >> out, "\nHEADERS:"
        pprint(dict(self.request.headers), out)
        
        message = out.getvalue()
        out.close()
        
        send_email(subject, message, 
                  self.conf.site.webmaster, 
                  self.conf.site.admin_mail.split(','))
        
    def is_secure(self):
        return self.request.headers.get('X-Scheme') == 'https'
    
    def prepare_render_context(self, **kwargs):
        kkwargs = {}
        if kwargs:
            kkwargs.update(kwargs)
        kkwargs['h'] = self.h
        kkwargs['c'] = self.c
        kkwargs['conf'] = self.conf
        return kkwargs
    
    def render_string(self, template_name, **kwargs):
        kkwargs = self.prepare_render_context(**kwargs)
        return tornado.web.RequestHandler.render_string(self, template_name, **kkwargs)
    
    def render(self, template_name, **kwargs):
        kkwargs = self.prepare_render_context(**kwargs)
        tornado.web.RequestHandler.render(self, template_name, **kkwargs)