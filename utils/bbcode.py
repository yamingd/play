## -*- coding: utf-8 -*-
import re

S_QUOTE_RE = re.compile(r"\s*\[quote\][\n\r]*(.+?)[\n\r]*\[\/quote\]\s*",re.I|re.U)
R_QUOTE_EXP = u"<div class=\"quote\"><span class=\"q\">\\1</span></div>"

S_CODE_RE = re.compile(r"\s*\[code\][\n\r]*(.+?)[\n\r]*\[\/code\]\s*",re.I|re.U)
R_CODE_EXP = u"<div class=\"code-block\">\\1</div>"

S_URL_RE = re.compile(r'''
        (?P<href>((src=["']|href=["'])\s*)*)((http|https)://
        (?:[%:\w]*@)?                           # authenticator
        (?P<domain>[a-z0-9\-]{1,63}\.)* # (sub)domain - alpha followed by 62max chars (63 total)
        (?P<tld>[a-z]{2,})                      # TLD
        (?::[0-9]+)?                            # port
        # files/delims/etc
        (?P<path>/[a-z0-9\-\._~:/\?#\[\]@!%\$&\'\(\)\*\+,;=]*)?)''', re.I | re.VERBOSE)
        
R_URL_EXP = u"<a href=\"\\1\" class='subject' target=\"_blank\">\\1</a>"

R_URL_EXP_0 = u"<a href=\"%s\" class='inner-link'>%s</a>"
R_URL_EXP_1 = u"<a href=\"%s\" class='external-link' target=\"_blank\">%s</a>"

S_STR = ['[b]','[/b]','[i]','[/i]','[u]','[/u]']
R_STR = ['<b>','</b>','<i>','</i>','<u>','</u>']
    
def rep_url(m):
    href = m.group('href')
    if not href:
        link = m.group(0)
        if link.startswith('http:') or link.startswith('https:'):
            return R_URL_EXP_1 % (link,link)
        else:
            return R_URL_EXP_0 % (link,link)
    return m.group(0)
    
def render(text):
    #print text
    #text = re.sub(S_QUOTE_RE,R_QUOTE_EXP,text)
    #print text
    #text = re.sub(S_CODE_RE,R_CODE_EXP,text)
    #print text
    text = re.sub(S_URL_RE,rep_url,text)
    #print text
    #for i in xrange(len(S_STR)):
    #    text = text.replace(S_STR[i],R_STR[i])
    #print text
    return text

if __name__ == '__main__':
    print render(u"this http://www.google.com ")
    print render(u"this <a href=\"http://www.google.com\">google.com</a>")
    print render(u"addd http://t.cn/hrZu7u ssss")
    print render(u"sss  http://sinaurl.cn/hqk8Vs sss")