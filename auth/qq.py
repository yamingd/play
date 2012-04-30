# -*- coding: utf-8 -*-
import logging
import urllib

from tornado import httpclient
from tornado import escape
from tornado.httputil import url_concat
from tornado.util import bytes_type, b

from tornado.auth import OAuthMixin, OAuth2Mixin

class QQWeiboMixin(OAuth2Mixin):
    _OAUTH_ACCESS_TOKEN_URL = "https://open.t.qq.com/cgi-bin/oauth2/access_token?"
    _OAUTH_AUTHORIZE_URL = "https://open.t.qq.com/cgi-bin/oauth2/authorize?"
    _OAUTH_NO_CALLBACKS = False
    _API_HOST = "https://open.t.qq.com/api"
    
    def get_authenticated_user(self, redirect_uri, client_id, client_secret,
                              code, callback, extra_fields=None):
        
        http = httpclient.AsyncHTTPClient()
        args = {
            "redirect_uri": redirect_uri,
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
        }

        fields = set(['openid', 'name', 'nick', 'head',
                    'location', 'isvip', 'isent', 'email'])
        if extra_fields: fields.update(extra_fields)

        http.fetch(self._oauth_request_token_url(**args),
          self.async_callback(self._on_access_token, redirect_uri, client_id,
                              client_secret, callback, fields))
    
    def _on_access_token(self, redirect_uri, client_id, client_secret,
                        callback, fields, response):
        if response.error:
            logging.warning('QQ Weibo auth error: %s' % str(response))
            callback(None)
            return

        args = escape.parse_qs_bytes(escape.native_str(response.body))
        session = {
            "access_token": args["access_token"][-1],
            "expires": args.get("expires")
        }

        self.qq_request(
            path="/user/info",
            callback=self.async_callback(
              self._on_get_user_info, callback, session, fields),
            access_token=session["access_token"],
            fields=",".join(fields)
          )
    
    def _on_get_user_info(self, callback, session, fields, user):
        """
        http://wiki.open.t.qq.com/index.php/%E5%B8%90%E6%88%B7%E7%9B%B8%E5%85%B3/%E8%8E%B7%E5%8F%96%E8%87%AA%E5%B7%B1%E7%9A%84%E8%AF%A6%E7%BB%86%E8%B5%84%E6%96%99
        """
        if user is None:
            callback(None)
            return
        user = user.get('data')
        fieldmap = {}
        for field in fields:
            fieldmap[field] = user.get(field)

        fieldmap.update({"access_token": session["access_token"], "session_expires": session.get("expires")})
        callback(fieldmap)
    
    def _on_qq_request(self, callback, response):
        if response.error:
            logging.warning("Error response %s fetching %s", response.error,
                            response.request.url)
            callback(None)
            return
        callback(escape.json_decode(response.body))
        
    def qq_request(self, path, callback, access_token=None,
                           post_args=None, **args):
        """Fetches the given relative API path, e.g., "/btaylor/picture"

        If the request is a POST, post_args should be provided. Query
        string arguments should be given as keyword arguments.

        An introduction to the Facebook Graph API can be found at
        http://developers.facebook.com/docs/api

        Many methods require an OAuth access token which you can obtain
        through authorize_redirect() and get_authenticated_user(). The
        user returned through that process includes an 'access_token'
        attribute that can be used to make authenticated requests via
        this method. Example usage::

            class MainHandler(tornado.web.RequestHandler,
                              tornado.auth.QQWeiboMixin):
                @tornado.web.authenticated
                @tornado.web.asynchronous
                def get(self):
                    self.qq_request(
                        "/me/feed",
                        post_args={"message": "I am posting from my Tornado application!"},
                        access_token=self.current_user["access_token"],
                        callback=self.async_callback(self._on_post))

                def _on_post(self, new_entry):
                    if not new_entry:
                        # Call failed; perhaps missing permission?
                        self.authorize_redirect()
                        return
                    self.finish("Posted a message!")

        """
        url = self._API_HOST + path
        all_args = {'format':'json'}
        if access_token:
            all_args["access_token"] = access_token
            all_args.update(args)
            all_args.update(post_args or {})
        if all_args: url += "?" + urllib.urlencode(all_args)
        callback = self.async_callback(self._on_qq_request, callback)
        http = httpclient.AsyncHTTPClient()
        if post_args is not None:
            http.fetch(url, method="POST", body=urllib.urlencode(post_args),
                       callback=callback)
        else:
            http.fetch(url, callback=callback)