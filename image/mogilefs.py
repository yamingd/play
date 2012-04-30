# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import traceback,sys,os
from cStringIO import StringIO
from Queue import Queue

from PIL import Image, ImageEnhance
from mogilefs_protocol import Client

from base import BaseImageFS

pil_format = {'.jpg':'JPEG','.jpeg':'JPEG','.jpe':'JPEG','.gif':'GIF','.png':'PNG'}

mog_queue = Queue()
class MogilefsClient(object):
    def __init__(self):
        pass
    def __enter__(self):
        self.c = mog_queue.get()
        return self.c
    
    def __exit__(self, _type, _value, _tb):
        mog_queue.task_done()
        mog_queue.put(self.c)

def build_mogilefs_clients(config):
    global mog_queue
    for i in range(config.pools):
        c = Client(domain=config.domain,trackers=config.trackers)
        mog_queue.put(c)
        
class MogileImageFS(BaseImageFS):
    """
    使用本地硬盘作为存储
    """
    def initfs(self):
        build_mogilefs_clients(self.settings)
        log.info('init MogileImageFS')
        
    def _save(self, im, ext, mc, key, quality=85):
        if im.mode != "RGB":
            im = im.convert("RGB")
        input = StringIO()
        imf = pil_format.get(ext.lower(),'JPEG')
        if quality:
            im.save(input,format=imf,quality=quality)
        else:
            im.save(input,format=imf)
        input.seek(0)
        mc.send_file(key, input)
        input.close()
    
    def save(self, im, path, quality=85):
        with MogilefsClient() as mc:
            file_name = os.path.split(path)[-1]
            name,pic_ext = os.path.splitext(file_name.encode("utf8"))
            self._save(im, pic_ext, mc, file_name)
            return file_name
        
    def remove(self, imageid, **kwargs):
        with MogilefsClient() as mc:
            sizes = kwargs['sizes']
            for w, h in sizes:
                mc.delete(self.get_imageurl(imageid, w, h))
                
    def get_imageurl(self, imageid, width, height):
        key = '%s.%sx%s' % (imageid,w,h)
        return key
    
    def thumb(self, imgwrap, sizes):
        oimg = imgwrap.image
        with MogilefsClient() as mc:
            if not imgwrap.imageid:
                imgwrap.imageid = self.newid()
            for w,h in sizes:
                key = self.get_imageurl(imgwrap.imageid, w, h)
                img2 = self._post_thumb(oimg,w,h)
                self._save(img2, imgwrap.image_ext, mc, key, quality=85)
            imgwrap.close()
            return imgwrap
    
    def crop(self, imgwrap, sizes):
        oimg = imgwrap.image
        with MogilefsClient() as mc:
            img = self._post_crop_image(oimg)
            if not imgwrap.imageid:
                imgwrap.imageid = self.newid()
            for w,h in sizes:
                key = self.get_imageurl(imgwrap.imageid, w, h)
                img2 = self._post_thumb(img,w,h)
                self._save(img2, imgwrap.image_ext, mc, key, quality=85)
            imgwrap.close()
            return imgwrap

image_proxy = MogileImageFS