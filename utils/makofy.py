# -*- coding: utf-8 -*-
from mako.template import Template
from mako.lookup import TemplateLookup

def config_mako_lookup(options):
    default_imports = ['from webhelpers.html import escape']
    if options.imports:
        default_imports.extend(options.imports)
    _mako_lookup = TemplateLookup(directories=[options.template_path],
                                     module_directory=options.mako_modules_dir,
                                     output_encoding='utf-8',
                                     default_filters=['escape'],
                                     encoding_errors='replace',
                                     imports=default_imports)
    return _mako_lookup