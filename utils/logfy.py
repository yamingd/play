# -*- coding: utf-8 -*-
import datetime
import logging
import logging.handlers
import re
import sys
import time

def setup_logger(options):
    """
    Turns on formatted logging output as configured.
    options = ODict({'log_folder':'','runtime':'','port':''})
    """
    options.log_file_prefix = "%s/%s.log" % (options.log_folder,options.port)
    options.log_file_max_size = 10*1000*1000
    options.log_file_num_backups = 30
    
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, options.logging.upper()))
    if options.log_file_prefix:
        channel = logging.handlers.RotatingFileHandler(
            filename=options.log_file_prefix,
            maxBytes=options.log_file_max_size,
            backupCount=options.log_file_num_backups)
        channel.setFormatter(_LogFormatter())
        root_logger.addHandler(channel)

    if (options.log_to_stderr or
        (options.log_to_stderr is None and not root_logger.handlers)):
        channel = logging.StreamHandler()
        channel.setFormatter(_LogFormatter())
        root_logger.addHandler(channel)

class _LogFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        logging.Formatter.__init__(self, *args, **kwargs)

    def format(self, record):
        try:
            record.message = record.getMessage()
        except Exception, e:
            record.message = "Bad message (%r): %r" % (e, record.__dict__)
        record.asctime = time.strftime(
            "%Y-%m-%d %H:%M:%S", self.converter(record.created))
        prefix = '[%(asctime)s %(levelname)s %(name)s:%(lineno)d]' % \
            record.__dict__
        formatted = prefix + " " + record.message
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            formatted = formatted.rstrip() + "\n" + record.exc_text
        return formatted.replace("\n", "\n    ")
