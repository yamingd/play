# -*- coding: utf-8 -*-
import mmhash
import os

csshash = {}
cssfiles = []
jshash = {}
jsfiles = []

CSS_FOLDER = u'public/static/css/'
JS_FOLDER = u'public/static/js/'

def hash_css():
    from quora import app_config
    folder = app_config.root_path
    folder = os.path.join(folder,CSS_FOLDER)
    for fname in os.listdir(folder):
        if not fname.endswith('.css'):
            continue
        fpath = folder + fname
        cssfiles.append(fpath)
        with open(fpath) as f:
            data = f.read()
            hash = mmhash.hash32(data)
            csshash[fname] = str(hash)
            #print fname,hash

def get_csshash(css):
    return csshash.get(css,'')

def hash_js():
    from quora import app_config
    folder = app_config.root_path
    folder = os.path.join(folder,JS_FOLDER)
    for fname in os.listdir(folder):
        if not fname.endswith('.js'):
            continue
        fpath = folder + fname
        jsfiles.append(fpath)
        with open(fpath) as f:
            data = f.read()
            hash = mmhash.hash32(data)
            jshash[fname] = str(hash)

def get_jshash(js):
    return jshash.get(js,'')

def minfiles(force=False):
    import os
    from quora import app_config
    root = app_config.root_path
    cmd = os.path.join(os.path.dirname(root),'bin/yuicompressor-2.3.5.jar')
    if force or app_config.site.runtime=='production':
        for cssfile in cssfiles:
            tmp = "java -jar %s %s -o %s" % (cmd,cssfile,cssfile)
            os.system(tmp)
        for jsfile in jsfiles:
            tmp = "java -jar %s %s -o %s" % (cmd,jsfile,jsfile)
            os.system(tmp)

def hash_and_minfile(minfile=False):
    hash_css()
    hash_js()
    minfiles(force=minfile)