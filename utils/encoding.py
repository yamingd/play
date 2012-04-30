# -*- coding: utf-8 -*-

def _force_unicode(text):
    if not text:
        return u''
    try:
        text = unicode(text, 'utf-8')
    except TypeError:
        try:
            text = unicode(text, 'gbk')
        except:
            text = unicode(text)
    return text

def _force_utf8(text):
    if not text:
        return u''
    return str(_force_unicode(text).encode('utf8'))