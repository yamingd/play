# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import traceback,sys,os
from cStringIO import StringIO

from PIL import Image, ImageEnhance

from play import app_global
from play.conf import settings
from play.utils.stringfy import url_valid

class ImageWrap(object):
    def __init__(self, imageid, fp, file_name):
        self.imageid = imageid
        self.fp = fp
        self.file_name = file_name
        name,pic_ext = os.path.splitext(file_name.encode("utf8"))
        self.image_ext = pic_ext
        self.name = name
        self.path = None
    
    @property
    def idname(self):
        return str(self.imageid)
    
    @property
    def image(self):
        return Image.open(self.fp)
    
    def close(self):
        self.fp.close()
        
class BaseImageFS(object):
    """
    图片处理、存储基类
    """
    def __init__(self, conf):
        self.namespace = 'image'
        self.settings = conf
        
        self.initfs()
    
    @property
    def idpool(self):
        """
        idpool负责产生唯一ID, 即redis
        """
        return app_global.redis
    
    def newid(self):
        rkey = '%s:next%sId' % (settings.redis.namespace, self.namespace)
        sid = self.idpool.incr(rkey)
        return sid
    
    def initfs(self):
        """
        在子类实现
        """
        pass
    
    def save(self,im, path, quality=85):
        """
        在子类实现
        """
        pass
    
    def remove(self, imageid, **kwargs):
        """
        在子类实现
        """
        pass
    
    def fromurl(self,url):
        if not url or not url_valid(url):
            log.info('invalid url:%s'% url)
            return None
        fp = StringIO()
        try:
            imgtype = get_image(url,fp)
            #print 'image type:',imgtype
            fp.seek(0)
            return ImageWrap(fp, imgtype)
        except:
            log.error('Reason:%s'% url)  
            log.exception('unexpected error')
            raise
    
    def get_imageurl(self, imageid, width, height):
        """
        在子类实现
        """
        pass
    
    def thumb(self, imgwrap, sizes):
        """
        在子类实现
        """
        pass
    
    def crop(self, imgwrap, sizes):
        """
        在子类实现
        """
        pass
    
    def _post_thumb(self, img, width, height):
        if not img or not width or not height:
            return img
        """Rescale the given image, optionally cropping it to
        make sure the result image has the specified width and height.
        """
        max_width = float(width)
        max_height = float(height)
        
        src_width, src_height = img.size
        dst_width, dst_height = max_width, max_height
        
        r = min(dst_width / src_width, dst_height / src_height)
        if r > 1:
            r = 1
        img2 = img.resize((int(src_width*r), int(src_height*r)), Image.ANTIALIAS)
        return img2

    def _post_crop_image(self, img):
        """
        make the image a square. Crop it.
        """
        src_width, src_height = img.size
        if src_width == src_height:
            return img
        #print src_width,src_height
        if src_width > src_height:
           delta = src_width - src_height
           left = int(delta/2)
           upper = 0
           right = src_height + left
           lower = src_height
        else:
           delta = src_height - src_width
           left = 0
           upper = int(delta)/2
           right = src_width
           lower = src_width + upper
        #print left,upper,right,lower
        im = img.crop((left, upper, right, lower))
        return im
    