# -*- coding: utf-8 -*-
import logging
import urllib

from tornado import httpclient
from tornado import escape
from tornado.httputil import url_concat
from tornado.util import bytes_type, b

from tornado.auth import OAuthMixin, OAuth2Mixin

class WeiboMixin(OAuthMixin):
    _OAUTH_REQUEST_TOKEN_URL = "http://api.t.sina.com/oauth/request_token"
    _OAUTH_ACCESS_TOKEN_URL = "http://api.t.sina.com/oauth/access_token"
    _OAUTH_AUTHORIZE_URL = "http://api.t.sina.com/oauth/authorize"
    _OAUTH_AUTHENTICATE_URL = "http://api.t.sina.com/oauth/authenticate"
    _OAUTH_NO_CALLBACKS = False
    _SINA_API_HOST = "http://api.t.sina.com/"
    
    def authenticate_redirect(self, callback_uri = None):
        """Just like authorize_redirect(), but auto-redirects if authorized.

        This is generally the right interface to use if you are using
        Twitter for single-sign on.
        """
        http = httpclient.AsyncHTTPClient()
        http.fetch(self._oauth_request_token_url(callback_uri = callback_uri), self.async_callback(
            self._on_request_token, self._OAUTH_AUTHENTICATE_URL, None))

    def sina_request(self, path, callback, access_token=None,
                           post_args=None, **args):
        """Fetches the given API path, e.g., "/statuses/user_timeline/btaylor"

        The path should not include the format (we automatically append
        ".json" and parse the JSON output).

        If the request is a POST, post_args should be provided. Query
        string arguments should be given as keyword arguments.

        All the Twitter methods are documented at
        http://apiwiki.twitter.com/Twitter-API-Documentation.

        Many methods require an OAuth access token which you can obtain
        through authorize_redirect() and get_authenticated_user(). The
        user returned through that process includes an 'access_token'
        attribute that can be used to make authenticated requests via
        this method. Example usage::

            class MainHandler(tornado.web.RequestHandler,
                              tornado.auth.SinaMixin):
                @tornado.web.authenticated
                @tornado.web.asynchronous
                def get(self):
                    self.sina_request(
                        "/statuses/update",
                        post_args={"status": "Testing Tornado Web Server"},
                        access_token=user["access_token"],
                        callback=self.async_callback(self._on_post))

                def _on_post(self, new_entry):
                    if not new_entry:
                        # Call failed; perhaps missing permission?
                        self.authorize_redirect()
                        return
                    self.finish("Posted a message!")

        """
        if path.startswith('http:') or path.startswith('https:'):
            # Raw urls are useful for e.g. search which doesn't follow the
            # usual pattern: http://search.twitter.com/search.json
            url = path
        else:
            url = self._SINA_API_HOST + path + ".json"
        # Add the OAuth resource request signature if we have credentials
        if access_token:
            all_args = {}
            all_args.update(args)
            all_args.update(post_args or {})
            method = "POST" if post_args is not None else "GET"
            oauth = self._oauth_request_parameters(
                url, access_token, all_args, method=method)
            args.update(oauth)
        if args: url += "?" + urllib.urlencode(args)
        callback = self.async_callback(self._on_sina_request, callback)
        http = httpclient.AsyncHTTPClient()
        if post_args is not None:
            http.fetch(url, method="POST", body=urllib.urlencode(post_args),
                       callback=callback)
        else:
            http.fetch(url, callback=callback)

    def _on_sina_request(self, callback, response):
        if response.error:
            logging.warning("Error response %s fetching %s", response.error,
                            response.request.url)
            callback(None)
            return
        callback(escape.json_decode(response.body))

    def _oauth_consumer_token(self):
        self.require_setting("sina_consumer_key", "Sina OAuth")
        self.require_setting("sina_consumer_secret", "Sina OAuth")
        return dict(
            key=self.settings["sina_consumer_key"],
            secret=self.settings["sina_consumer_secret"])

    def _oauth_get_user(self, access_token, callback):
        callback = self.async_callback(self._parse_user_response, callback)
        self.sina_request(
            "/users/show",
            access_token=access_token, callback=callback, 
            screen_name=access_token["screen_name"])

    def _parse_user_response(self, callback, user):
        if user:
            user["username"] = user["screen_name"]
        callback(user)

class Weibo2Mixin(OAuth2Mixin):
    _OAUTH_ACCESS_TOKEN_URL = "https://api.weibo.com/oauth2/access_token?"
    _OAUTH_AUTHORIZE_URL = "https://api.weibo.com/oauth2/authorize?"
    _OAUTH_NO_CALLBACKS = False
    _API_HOST = "https://api.weibo.com/2"
    
    def get_authenticated_user(self, redirect_uri, client_id, client_secret,
                              code, callback, extra_fields=None):
        
        http = httpclient.AsyncHTTPClient()
        args = {
            "redirect_uri": redirect_uri,
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
        }

        fields = set(['id', 'name', 'screen_name', 'profile_image_url', 'url',
                    'location'])
        if extra_fields: fields.update(extra_fields)

        http.fetch(self._oauth_request_token_url(**args),
          self.async_callback(self._on_access_token, redirect_uri, client_id,
                              client_secret, callback, fields))
    
    def _on_access_token(self, redirect_uri, client_id, client_secret,
                        callback, fields, response):
        if response.error:
            logging.warning('Sina Weibo2 auth error: %s' % str(response))
            callback(None)
            return

        args = escape.parse_qs_bytes(escape.native_str(response.body))
        session = {
            "access_token": args["access_token"][-1],
            "expires": args.get("expires")
        }

        self.qq_request(
            path="/users/show",
            callback=self.async_callback(
              self._on_get_user_info, callback, session, fields),
            access_token=session["access_token"],
            fields=",".join(fields)
          )
    
    def _on_get_user_info(self, callback, session, fields, user):
        
        if user is None:
            callback(None)
            return
        fieldmap = {}
        for field in fields:
            fieldmap[field] = user.get(field)

        fieldmap.update({"access_token": session["access_token"], "session_expires": session.get("expires")})
        callback(fieldmap)
    
    def _on_sina_request(self, callback, response):
        if response.error:
            logging.warning("Error response %s fetching %s", response.error,
                            response.request.url)
            callback(None)
            return
        callback(escape.json_decode(response.body))
        
    def sina_request(self, path, callback, access_token=None,
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
                              tornado.auth.Weibo2Mixin):
                @tornado.web.authenticated
                @tornado.web.asynchronous
                def get(self):
                    self.sina_request(
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
        url = self._API_HOST + path + ".json"
        if access_token:
            all_args["access_token"] = access_token
            all_args.update(args)
            all_args.update(post_args or {})
        if all_args: url += "?" + urllib.urlencode(all_args)
        callback = self.async_callback(self._on_sina_request, callback)
        http = httpclient.AsyncHTTPClient()
        if post_args is not None:
            http.fetch(url, method="POST", body=urllib.urlencode(post_args),
                       callback=callback)
        else:
            http.fetch(url, callback=callback)