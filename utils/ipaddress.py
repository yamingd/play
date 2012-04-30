# -*- coding: utf-8 -*-

def to_number(ip_string):
    if ip_string is None or len(ip_string)==0:
        return None
    octets = ip_string.strip().split('.')
    octets.reverse()
    ip_num = 0
    for i,octet in enumerate(octets):
        ip_num += int(octet)*math.pow(256,i)
    return int(ip_num)

def to_ip(num):
    if num is None or num <= 0:
        return None
    octets = [0,0,0,0]
    for i,octet in enumerate(octets):
        octets[i] = int(num / math.pow(256,3-i))
        num = int(num % math.pow(256,3-i))
    octets = [str(octet) for octet in octets]
    return ".".join(octets)