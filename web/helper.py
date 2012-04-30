# -*- coding: utf-8 -*-
from play.utils import bbcode
from datetime import timedelta,datetime
from webhelpers.html import *
from webhelpers.date import *
from webhelpers.html.converters import nl2br,markdown

def datetime_words(dt):
    if dt is None:
        return ""   
    if isinstance(dt,long) or isinstance(dt,int) or isinstance(dt,float):
        dt = datetime.fromtimestamp(float(dt))
    td = datetime.now()-dt
    days = td.days
    secs = td.seconds
    if days>0:
        if days == 1:
            return u'昨天'
        elif days == 2:
            return u'前天'
        else:
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        #print dt,td
        h = secs / 3600
        m = (secs-h*3600)/60
        secs = secs - h*3600 - m*60
        if h>0:
            return u'%s小时前' % h
        if m>0:
            return u'%s分钟前' % m
        if secs <= 15:
            return u'刚刚'
        return u'%s秒前' % secs

def rte_content(text,bbcode=True):
    if not text:
        return u''
    if bbcode:
        text = bbcode.render(unicode(text.encode('utf8'),'utf8'))
    text = literal(text)
    text = nl2br(text)
    return text

def tag_fontsize(max,min,amount,min_font=13,max_font=31,step=4):
    """
    find min freq and max freq
    x =  freq of tag we want to calculate the font size
    scaling factor, K = (x – min freq) / (max freq – min freq)
    font range = max font size – min font size
    font step = C  (the constant font step size)
    font for tag =    min font size  + (C * floor (K * (font range/ C)))
    """
    max = int(max or 0)
    min = int(min or 0)
    diff = max - min
    if diff == 0:
        return min_font
    if amount < min:
        return 12
    k  = math.fabs(amount - min) * 1.0/diff
    font = min_font + (step * math.floor(k * (max_font-min_font)*1.0/step))
    return int(font)

def html_quote(text):
    return text.replace('<','&lt').replace('>','&gt').replace('"','&quot;')