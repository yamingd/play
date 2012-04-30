# -*- coding: utf-8 -*-
# python
import types
import re
import os, sys
import logging
from datetime import datetime

# tornado
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options

from tornado import httpclient
httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

#app
from play.utils import logfy, make_storage
from play.utils import importlib
from play.web import route
from play.model import model_manager
from play.model.base import BaseModel
from play import mq, app_global

################################################################################
define("debug", default=False, help="run in debug mode", type=bool)
define("port", default=8000, help="run on the given port", type=int)
define("prefork", default=False, help="pre-fork across all CPUs", type=bool)
define("showurls", default=False, help="Show all routed URLs", type=bool)
define("dont_combine", default=False, help="Don't combine static resources", type=bool)
define("queue", default=None, help="Run mq server", type=str)

class Application(tornado.web.Application):
    def __init__(self,root,settings,xsrf_cookies=True):
        self.root = root
        self.app_settings = settings
        ui_modules_map = self._load_ui_module()
        handlers = route.get_routes()
        app_settings = dict(
            title=settings.site.title,
            template_path=os.path.join(root, "apps", "templates"),
            static_path=os.path.join(root, "static"),
            ui_modules=ui_modules_map,
            xsrf_cookies=xsrf_cookies,
            cookie_secret=settings.site.cookie_secret,
            login_url=settings.site.login_url,
            debug=options.debug,
            optimize_static_content=settings.site.optimize_static_content,
            webmaster=settings.site.webmaster,
            feedback=settings.site.feedback,
            closure_location=os.path.join(root, "static", "compiler.jar"),
            yui_location=os.path.join(root, "static", "yuicompressor-2.4.2.jar"),
            cdn_prefix = settings.site.cdn
        )
        
        self._load_handlers()
        self._load_models()
        self._load_mq_handlers()
        
        tornado.web.Application.__init__(self, handlers, **app_settings)
        
    def _load_ui_module(self):
        logging.info('loading ui modeuls')
        ui_modules_map = {}
        for app_name in self.app_settings.apps:
            _ui_modules = __import__('apps.%s' % app_name, globals(), locals(),
                                     ['ui_modules'], -1)
            try:
                ui_modules = _ui_modules.ui_modules
            except AttributeError:
                # this app simply doesn't have a ui_modules.py file
                continue
            
            for name in [x for x in dir(ui_modules) if re.findall('[A-Z]\w+', x)]:
                thing = getattr(ui_modules, name)
                logging.info(thing)
                try:
                    if issubclass(thing, tornado.web.UIModule):
                        ui_modules_map[name] = thing
                except TypeError:
                    # most likely a builtin class or something
                    pass
        return ui_modules_map
    
    def _load_models(self):
        """
        Database Model.
        """
        logging.info('loading database model')
        for app_name in self.app_settings.apps:
            _models = __import__('apps.%s' % app_name, globals(), locals(),
                                     ['models'], -1)
            try:
                models = _models.models
            except AttributeError:
                # this app simply doesn't have a models.py file
                continue
            logging.info(models)
            model_ids = []
            for name in [x for x in dir(models) if re.findall('[A-Z]\w+', x)]:
                thing = getattr(models, name)
                logging.info(thing)
                try:
                    if issubclass(thing, BaseModel):
                        """
                        models.User.ops.Add, models.User.ops.Update
                        """
                        if thing.model_id>0 and thing.model_id in model_ids:
                            raise Exception("%s model_id must be unique")
                        model_ids.append(thing.model_id)
                        thing.ops = make_storage(thing.model_ops, base=thing.model_id*100)
                        model_manager.add(thing)
                except TypeError:
                    # most likely a builtin class or something
                    pass
        
        setattr(app_global, 'models', model_manager)
        
    def _load_handlers(self):
        logging.info('loading http request handlers')
        for app_name in self.app_settings.apps:
            __import__('apps.%s' % app_name, globals(), locals(), ['handlers'], -1)
    
    def _load_mq_handlers(self):
        logging.info('loading mq handlers')
        for app_name in self.app_settings.apps:
            _models = __import__('apps.%s' % app_name, globals(), locals(), ['mq'], -1)
            try:
                models = _models.mq
            except AttributeError:
                # this app simply doesn't have a models.py file
                continue
            logging.debug(models)
            for name in [x for x in dir(models) if re.findall('[A-Z]\w+', x)]:
                thing = getattr(models, name)
                logging.debug(thing)
                try:
                    if issubclass(thing, mq.Message):
                        model_manager.add(thing)
                except TypeError:
                    # most likely a builtin class or something
                    pass
                
def setup_environ(args, settings_mod):
    logging.info('setup environ')
    args.append("-logging=none")
    tornado.options.parse_command_line(args)
    
    if '__init__.py' in settings_mod.__file__:
        p = os.path.dirname(settings_mod.__file__)
    else:
        p = settings_mod.__file__
    project_directory, settings_filename = os.path.split(p)
    if project_directory == os.curdir or not project_directory:
        project_directory = os.getcwd()
    project_name = os.path.basename(project_directory)

    # Strip filename suffix to get the module name.
    settings_name = os.path.splitext(settings_filename)[0]

    # Strip $py for Jython compiled files (like settings$py.class)
    if settings_name.endswith("$py"):
        settings_name = settings_name[:-3]

    # Set PLAY_SETTINGS_MODULE appropriately.
    # If PLAY_SETTINGS_MODULE is already set, use it.
    os.environ['PLAY_SETTINGS_MODULE'] = os.environ.get(
        'PLAY_SETTINGS_MODULE',
        '%s.%s' % (project_name, settings_name)
    )

    # Import the project module. We add the parent directory to PYTHONPATH to
    # avoid some of the path errors new users can have.
    sys.path.append(os.path.join(project_directory, os.pardir))
    importlib.import_module(project_name)
    sys.path.pop()
    
    #logging
    options.log_folder = os.path.join(settings_mod.root, "log")
    options.logging = settings_mod.site.logging
    logfy.setup_logger(options)
    #load modules
    register_module()
    #
    return project_directory

def register_module():
    """
    module should have attr {key}_proxy, key should be the configuration's key
    """
    from play.conf import settings
    logging.info('register global module')
    for key in settings.keys:
        conf = getattr(settings, key)
        if hasattr(conf, 'engine'):
            m = importlib.import_module(conf.engine)
            if not m:
                logging.info('missing module:%s' % conf.engine)
                continue
            func = getattr(m, key+"_proxy")
            if func:
                app_global.set(key, conf, func)
            else:
                logging.info('module %s miss attr %s_proxy' % (conf.engine, key))
    
    
def start_web(args, settings):    
    if options.showurls:
        for each in route.get_routes():
            print each._path.ljust(60),
            print each.handler_class.__name__
        return
    
    http_server = tornado.httpserver.HTTPServer(Application(settings.root, settings))
    print datetime.now(),"Starting tornado on port", options.port
    if options.prefork:
        print "\tpre-forking"
        http_server.bind(options.port)
        http_server.start()
    else:
        http_server.listen(options.port)

    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        pass

def start_mq(args, settings):
    #start
    from play.conf import settings
    from play.mq import consumer
    from twisted.internet import reactor      
      
    backend = None
    task = ConsumerBase(backend, options.queue)
    
    reactor.callLater(0.1, task.start)
    
    try:
        print 'application is start at ', datetime.now()
        print "Consumer running, press Ctrl+C to quit"
        reactor.run()
    except KeyboadInterrupt, ki:
        print "Bye bye!"
        reactor.stop()