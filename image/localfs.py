# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import traceback,sys,os
import hashlib

from PIL import Image, ImageEnhance

from base import BaseImageFS
from play.conf import settings

class LocalImageFS(BaseImageFS):
    """
    使用本地硬盘作为存储
    """
    def initfs(self):
        self.root_folder = self.settings.root_folder
        log.info('init LocalImageFS, root=' + self.root_folder)
        
    def save(self, im, path, quality=85):
        if im.mode != "RGB":
            im = im.convert("RGB")
        if quality:
            im.save(path,quality=quality)
        else:
            im.save(path)
    
    def hexpath(self, root, imageid):
        idhex = hashlib.md5(str(imageid)).hexdigest()
        temp = [idhex[i]+idhex[2]+idhex[4] for i in xrange(0,24,6)]
        return os.path.join(root,*temp)
    
    def remove(self, imageid, **kwargs):
        os.remove(self.hexpath(self.root_folder, imageid))
       
    def _get_save_folder(self, imageid):
        to_folder = self.hexpath(self.root_folder, imageid)
        if not os.path.exists(to_folder):
            os.makedirs(to_folder)
        return to_folder
    
    def get_imageurl(self, imageid, width, height):
        path = self.hexpath('/', imageid)
        path = os.path.join(path, str(imageid)+'.%sx%s' % (w,h))
        return path
    
    def thumb(self, imgwrap, sizes):
        oimg = imgwrap.image
        if not imgwrap.imageid:
            imgwrap.imageid = self.newid()
        imgwrap.path = self._get_save_folder(imgwrap.imageid)
        for w,h in sizes:
            img2 = self._post_thumb(oimg,w,h)
            path = os.path.join(imgwrap.path, imgwrap.idname+'.%sx%s' % (w,h))
            self.save(img2, path)
        imgwrap.close()
        return imgwrap
    
    def crop(self, imgwrap, sizes):
        oimg = imgwrap.image
        img = self._post_crop_image(oimg)
        if not imgwrap.imageid:
            imgwrap.imageid = self.newid()
        imgwrap.path = self._get_save_folder(imgwrap.imageid)
        for w,h in sizes:
            img2 = self._post_thumb(img,w,h)
            path = os.path.join(imgwrap.path, imgwrap.idname+'.%sx%s' % (w,h))
            self.save(img2, path)
        imgwrap.close()
        return imgwrap

image_proxy = LocalImageFS