# -*- coding: utf-8 -*-
import logging
import functools

class AsyncClass(object):
    
    def async_callback(self, callback, *args, **kwargs):
        if callback is None:
            return None
        if args or kwargs:
            callback = functools.partial(callback, *args, **kwargs)
        def wrapper(*args, **kwargs):
            try:
                return callback(*args, **kwargs)
            except Exception, e:
                logging.error("Exception after headers written",
                                  exc_info=True)
        return wrapper


def dumy_callback(*args,**kwargs):
    pass
