# -*- coding: utf-8 -*-
import os
import logging
log = logging.getLogger(__name__)

def check_service(service):
    cmd = 'ps aux | grep %s' % service
    result = os.popen(cmd).read().split('\n')
    status = len(result)>3
    #print cmd, status
    #log.info(result)
    if not status:
        print 'service [%s] is not running' % service
    log.info('service %s running? %s' % (service,status))
    return status

def run_service(service):
    try:
        cmd = 'sudo service %s start' % service
        result = os.popen(cmd).read()
        log.info('run service: %s, %s' % (service,result))
    except:
        log.exception('run_service error:')