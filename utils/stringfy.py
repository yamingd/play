# -*- coding: utf-8 -*-
import random, re
import zlib
from urlparse import urlparse, urlunparse

import tornado

from encoding import *

def randstr(len, reallyrandom = False):
    """If reallyrandom = False, generates a random alphanumeric string
    (base-36 compatible) of length len.  If reallyrandom, add
    uppercase and punctuation (which we'll call 'base-93' for the sake
    of argument) and suitable for use as salt."""
    alphabet = 'abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    if reallyrandom:
        alphabet += '!#$%&\()*+,-./:;<=>?@[\\]^_{|}~'
    return ''.join(random.choice(alphabet)
                   for i in range(len))
    
def string2js(s):
    """adapted from http://svn.red-bean.com/bob/simplejson/trunk/simplejson/encoder.py"""
    ESCAPE = re.compile(r'[\x00-\x19\\"\b\f\n\r\t]')
    ESCAPE_ASCII = re.compile(r'([\\"/]|[^\ -~])')
    ESCAPE_DCT = {
        # escape all forward slashes to prevent </script> attack
        '/': '\\/',
        '\\': '\\\\',
        '"': '\\"',
        '\b': '\\b',
        '\f': '\\f',
        '\n': '\\n',
        '\r': '\\r',
        '\t': '\\t',
    }
    for i in range(20):
        ESCAPE_DCT.setdefault(chr(i), '\\u%04x' % (i,))

    def replace(match):
        return ESCAPE_DCT[match.group(0)]
    return '"' + ESCAPE.sub(replace, s) + '"'

def to_js(content, callback="document.write", escape=True):
    before = after = ''
    if callback:
        before = callback + "("
        after = ");"
    if escape:
        content = string2js(content)
    return before + content + after

r_base_url = re.compile("(?i)(?:.+?://)?(?:www[\d]*\.)?([^#]*[^#/])/?")
def base_url(url):
    res = r_base_url.findall(url)
    return (res and res[0]) or url

r_domain = re.compile("(?i)(?:.+?://)?(?:www[\d]*\.)?([^/:#?]*)")
def domain(s):
    """
        Takes a URL and returns the domain part, minus www., if
        present
    """
    res = r_domain.findall(s)
    domain = (res and res[0]) or s
    return domain.lower()

r_path_component = re.compile(".*?/(.*)")
def path_component(s):
    """
        takes a url http://www.foo.com/i/like/cheese and returns
        i/like/cheese
    """
    res = r_path_component.findall(base_url(s))
    return (res and res[0]) or s

valid_schemes = ('http', 'https', 'ftp', 'mailto')         
valid_dns = re.compile('^[-a-zA-Z0-9]+$')
def sanitize_url(url, require_scheme = False):
    """Validates that the url is of the form

    scheme://domain/path/to/content#anchor?cruft

    using the python built-in urlparse.  If the url fails to validate,
    returns None.  If no scheme is provided and 'require_scheme =
    False' is set, the url is returned with scheme 'http', provided it
    otherwise validates"""

    if not url:
        return

    url = url.strip()
    if url.lower() == 'self':
        return url

    u = urlparse(url)
    # first pass: make sure a scheme has been specified
    if not require_scheme and not u.scheme:
        url = 'http://' + url
        u = urlparse(url)

    if u.scheme and u.scheme in valid_schemes:
        labels = u.hostname.split('.')
        for label in labels:
            try:
                #if this succeeds, this portion of the dns is almost
                #valid and converted to ascii
                label = label.encode('idna')
            except UnicodeError:
                return
            else:
                #then if this success, this portion of the dns is really valid
                if not re.match(valid_dns, label):
                    return
        return url

_alphabet = '0123456789abcdefghijklmnopqrstuvwxyz'
def to_base(q, alphabet):
    if q < 0: raise ValueError, "must supply a positive integer"
    l = len(alphabet)
    converted = []
    while q != 0:
        q, r = divmod(q, l)
        converted.insert(0, alphabet[r])
    return "".join(converted) or '0'

def to36(q):
    return to_base(q, _alphabet)
    
def query_string(dict):
    pairs = []
    for k,v in dict.iteritems():
        if v is not None:
            try:
                k = tornado.escape.url_escape(unicode(k).encode('utf-8'))
                v = tornado.escape.url_escape(unicode(v).encode('utf-8'))
                pairs.append(k + '=' + v)
            except UnicodeDecodeError:
                continue
    if pairs:
        return '?' + '&'.join(pairs)
    else:
        return ''

def safe_eval_str(unsafe_str):
    return unsafe_str.replace('\\x3d', '=').replace('\\x26', '&')

rx_whitespace = re.compile('\s+', re.UNICODE)
rx_notsafe = re.compile('\W+', re.UNICODE)
rx_underscore = re.compile('_+', re.UNICODE)
def title_to_url(title, max_length = 50):
    """Takes a string and makes it suitable for use in URLs"""
    title = _force_unicode(title)           #make sure the title is unicode
    title = rx_whitespace.sub('-', title)   #remove whitespace
    title = rx_notsafe.sub('', title)       #remove non-printables
    title = rx_underscore.sub('-', title)   #remove double underscores
    title = title.strip('-')                #remove trailing underscores
    title = title.lower()                   #lowercase the title

    if len(title) > max_length:
        #truncate to nearest word
        title = title[:max_length]
        last_word = title.rfind('-')
        if last_word > 0:
            title = title[:last_word]
    return title

def clean_comma_dot(txt,result='raw'):
    txt = txt.replace(u'，',',').replace(u'；',',').replace(u';',',')
    if result=='list':
        return txt.split(',')
    return txt

usernameRE = re.compile(r"^[^ \t\n\r@<>()]+$", re.I)
domainRE = re.compile(r'''
    ^(?:[a-z0-9][a-z0-9\-]{0,62}\.)+ # (sub)domain - alpha followed by 62max chars (63 total)
    [a-z]{2,}$                       # TLD
''', re.I | re.VERBOSE)
def email_valid(value):
    if not value:
        return False
    value = value.strip()
    splitted = value.split('@', 1)
    try:
        username, domain=splitted
        if not usernameRE.search(username):
            return False
        if not domainRE.search(domain):
            return False
        return True
    except ValueError:
        return False

url_re = re.compile(r'''
        ^(http|https)://
        (?:[%:\w]*@)?                           # authenticator
        (?P<domain>[a-z0-9][a-z0-9\-]{1,62}\.)* # (sub)domain - alpha followed by 62max chars (63 total)
        (?P<tld>[a-z]{2,})                      # TLD
        (?::[0-9]+)?                            # port

        # files/delims/etc
        (?P<path>/[a-z0-9\-\._~:/\?#\[\]@!%\$&\'\(\)\*\+,;=]*)?
        $
    ''', re.I | re.VERBOSE)
url_scheme_re = re.compile(r'^[a-zA-Z]+:')
def url_valid(url):
    url = url.strip()
    if not url_scheme_re.search(url):
        url = 'http://' + url
    match = url_scheme_re.search(url)
    value = match.group(0).lower() + url[len(match.group(0)):]
    match = url_re.search(value)
    if not match:
        return None
    return url

def to_int(num,default_val=None):
    if not num:
        return default_val
    if num.isdigit():
        return int(num)
    return None

def hash_32(txt):
    if txt is None:
        return None
    return zlib.crc32(txt.strip().lower().encode('utf8'))