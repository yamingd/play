# -*- coding: utf-8 -*-
import hashlib

_base36 = '0123456789abcdefghijklmnopqrstuvwxyz'
_hex_salt = u'yamingd:%s:@gmail.com'
_base_mod = 8
_char_len = 6

def get(full_url):
    hex = hashlib.md5(_hex_salt % full_url.encode('utf8')).hexdigest()
    hexlen = len(hex)
    subHexLen = hexlen / _base_mod
    codes = []
    for i in range(subHexLen):
        subHex = hex[i*_base_mod:(i+1)*_base_mod]
        intval = 0x3FFFFFFF & long('0x'+subHex,base=16)
        code = []
        for j in range(_char_len):
            val = 0x0000001F & intval
            code.append(_base36[val])
            intval = intval >> 5
        codes.append("".join(code))
    return codes

if __name__ == '__main__':
    get(u'http://www.snippetit.com/1')