# -*- coding: utf-8 -*-
import logging
import functools

from pysolr import Solr, SolrJson, SolrError, TermVectorResult, GroupedResults
from solrcoreadmin import SolrCoreAdmin

from tornado import httpclient

from base import BaseSearchBackend, log_query, ResultSet, FacetSet
from play.utils import async

class AsyncSolr(SolrJson, async.AsyncClass):    
    def _on_send_request(self, callback, response):
        if response.error:
            logging.warning("Error response %s fetching %s", response.error,
                            response.request.url)
            callback(SolrError(self._extract_error(dict(response.getheaders()), response.read())))
            return
        callback(response.read())
    
    def _send_request(self, method, path, callback, body=None, headers=None):
        if headers is None:
            headers = {}
        
        url = 'http://' + self.host
        if self.port:
            url = url + ':' + self.port
        url = url + path
        callback = self.async_callback(self._on_send_request, callback)
        http = httpclient.AsyncHTTPClient()
        if body is not None:
            http.fetch(url, method="POST", body=body, callback=callback, headers=headers)
        else:
            http.fetch(url, callback=callback, headers=headers)
    
    def _select(self, params, callback):
        # encode the query as utf-8 so urlencode can handle it
        params['q'] = self._encode_q(params['q'])
        params['wt'] = 'json' # specify json encoding of results
        path = '%s/select/?%s' % (self.path, urlencode(params, True))
        self._send_request('GET', path, callback)

    def _select_post(self, params, callback):
        """
        Send a query via HTTP POST. Useful when the query is long (> 1024 characters)
        """
        params['q'] = self._encode_q(params['q'])
        params['wt'] = 'json' 
        path = '%s/select?' % (self.path,)
        
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        body = urlencode(params, False)
        self._send_request('POST', path, callback, body=body, headers=headers)
    
    def _mlt(self, params, callback):
        # encode the query as utf-8 so urlencode can handle it
        params['q'] = self._encode_q(params['q'])
        params['wt'] = 'json' # specify json encoding of results
        path = '%s/mlt?%s' % (self.path, urlencode(params, True))
        self._send_request('GET', path, callback)

    def _tvrh(self, params, callback):
        # encode the query as utf-8 so urlencode can handle it
        params['q'] = self._encode_q(params['q'])
        params['wt'] = 'json' # specify json encoding of results
        path = '%s/tvrh?%s' % (self.path, urlencode(params, True))
        self._send_request('GET', path, callback)
    
    def _update(self, message, callback, commit=True):
        """
        Posts the given xml message to http://<host>:<port>/solr/update/json and
        returns the result.
        """
        path = '%s/update/json' % self.path
        if commit:
            path = path + "?commit=true"
        self._send_request('POST', path, callback, message, {'Content-type': 'application/json'})
    
    def _on_search(self, callback, response):
        if response is SolrError:
            callback(response)
            return
        callback(self.result_class(response, decoder=self.decoder))
    
    def search(self, q, callback, **kwargs):
        """Performs a search and returns the results.  query input can be a list
        
        Examples::
            
            conn.search('ipod')
            
            conn.search(["ipod","category_id:1"],facet="on", 
                **{'facet.field':['text','tags','cat','manufacturer'],'rows':10})
        """
        params = {'q': q}
        params.update(kwargs)
        callback = self.async_callback(self._on_search, callback)
        if len(q) < 1024:
            self._select(params, callback)
        else:
            self._select_post(params, callback)
    
    def _on_more_like_this(self, callback, response):
        if response is SolrError:
            callback(response)
            return
        result = self.decoder.decode(response)
        if result['response'] is None:
            result['response'] = {
                'docs': [],
                'numFound': 0,
            }
        result = self.result_class(response, decoder=self.decoder)
        callback(result)
        
    def more_like_this(self, q, mltfl, callback, **kwargs):
        """
        Finds and returns results similar to the provided query.
        
        Requires Solr 1.3+.
        """
        params = {
            'q': q,
            'mlt.fl': mltfl,
        }
        params.update(kwargs)
        callback = self.async_callback(self._on_more_like_this, callback)
        self._mlt(params, callback)
    
    def _on_term_vectors(self, callback, field, response):
        if response is SolrError:
            callback(response)
            return
        callback(TermVectorResult(field,response))
        
    def term_vectors(self,q, callback, field=None,**kwargs):
        params = {'q': q or '','tv.all':'true' }
        if field:
            params['tv.fl'] = field
        params.update(kwargs)
        callback = self.async_callback(self._on_term_vectors, callback, field)
        self._tvrh(params, callback)
    
    def _on_group(self, callback, response):
        if response is SolrError:
            callback(response)
            return
        callback(GroupedResults(response))
        
    def group(self, q, callback, **kwargs):
        params = {'q': q or '',
                  'group':'true' }
        
        params.update(kwargs)
        callback = self.async_callback(self._on_group, callback)
        self._select(params, callback)
    
    def _on_add(self, callback, response):
        callback(response)
    
    def add(self, docs, callback):
        message = json.dumps(docs)
        callback = self.async_callback(self._on_add, callback)
        self._update(message, callback, commit=True)
    
    def _on_delete(self, callback, response):
        callback(response)
    
    def delete(self, callback, id=None, q=None, fromPending=True, fromCommitted=True):
        """Deletes documents."""
        if id is None and q is None:
            raise ValueError('You must specify "id" or "q".')
        elif id is not None and q is not None:
            raise ValueError('You many only specify "id" OR "q", not both.')
        elif id is not None:
            m = json.dumps({"delete":{"id":"%s" % id }}) 
        elif q is not None:
            m = json.dumps({"delete":{"query":"%s" % q }}) 
        
        callback = self.async_callback(self._on_delete, callback)
        self._update(m, callback, commit=True)

    def commit(self):
        pass
    
    def _on_optimize(self, response):
        logging.info('optimize')
    
    def optimize(self, waitFlush=False, waitSearcher=False, block=False):
        """
        Optimize index and optionally wait for the call to be completed before returning with `block=True`. Default
        is `False`
        """
        params = {'waitFlush':str(waitFlush).lower(),'waitSearcher':str(waitSearcher).lower(),'optimize':str(True).lower()}
        path = '%s/update?%s' % (self.path, urlencode(params))
        callback = self.async_callback(self._on_optimize)
        self._send_request('GET', path, callback)
        
class SolrBackend(BaseSearchBackend, async.AsyncClass):
    def __init__(self, conf):
        self.conf = conf
        self.engine = 'Solr'
        self.init()
    
    def init(self):
        self.conn = AsyncSolr(self.conf.url)
        
    def _on_add(self, callback, response):
        callback(response)
        
    def add(self, docs, callback=async.dumy_callback):
        """
        docs = [{'id':1,'text':u'abccc','category_fi':[],'tag_fi':[]}]
        """
        if not isinstance(docs, list):
            docs = list(docs)
        for item in docs:
            if not 'id' in item:
                raise Exception('Please provide id property.')
        callback = self.async_callback(self._on_add, callback)
        self.conn.add(docs, callback)
    
    def _on_update(self, callback, response):
        callback(response)
        
    def update(self, docs, callback=async.dumy_callback):
        """
        docs = [{'id':1,'text':u'abccc','category_fi':[],'tag_fi':[]}]
        """
        if not isinstance(docs, list):
            docs = list(docs)
        for item in docs:
            if not 'id' in item:
                raise Exception('Please provide id property.')
        callback = self.async_callback(self._on_update, callback)
        self.conn.update(docs, callback)
    
    def _on_search(self, callback, wrapper, page, size, response):
        if response is SolrError:
            callback(None,None)
            return
        docs = response.docs
        total = response.hits
        facets = response.facets
        ret = ResultSet(docs, wrapper=wrapper, total=total, page=page, size=size)
        callback(ret, FacetSet(facets))
    
    @log_query
    def search(self, q, callback, page=1, size=10, sorts=[], fields=[], facet=None, facet_fields=[], wrapper=None):
        """Performs a search and returns the results.  query input can be a list
        Examples::
            sorts = ["id desc","create_time_i asc"]
            conn.search('ipod')
            conn.search(["ipod","category_fi:1"],facet="on", 
                **{'facet.field':['text','tags','cat','manufacturer'],'rows':10})
        """
        params = {}
        if sorts:
            params['sort'] = ','.join(sorts)
        if facet and facet_fields:
            params['facet'] = 'on'
            params['facet.field'] = facet_fields
        params['start'] = (page-1)*size
        params['rows'] = size
        if 'id' not in fields:
            fields.insert(0, 'id')
        params['fl'] = ','.join(fields)
        
        callback = self.async_callback(self._on_search, callback, wrapper, page, size)
        self.conn.search(q, callback, **params)
    
    def _on_more_like_this(self, callback, wrapper, response):
        if response is SolrError:
            callback(None)
            return
        docs = response.docs
        total = response.hits
        ret = ResultSet(docs, wrapper=wrapper, total=total, page=1, size=total)
        callback(ret)
    
    @log_query
    def more_like_this(self, q, callback, size=10, fields=[], wrapper=None):
        params = {}
        params['mlt.count'] = size
        params['mlt'] = true
        
        callback = self.async_callback(self._on_more_like_this, callback, wrapper)
        self.conn.more_like_this(q, callback, ','.join(fields), **params)
    
    def _on_term_suggest(self, callback, response):
        if response is SolrError:
            callback(None)
            return
        facets = result.facets
        facets = facets['facet_fields'] if facets else {}
        callback(facets)
        
    @log_query
    def term_suggest(self, q, prefix, fields, callback, limit=10, mincount=1):
        params = {}
        params['facet'] = 'on'
        params['facet.field'] = fields
        params['facet.prefix'] = prefix
        params['facet.limit'] = limit
        params['facet.mincount'] = mincount
        params['rows'] = 0
        
        callback = self.async_callback(self._on_term_suggest, callback)
        self.conn.search(q, callback, **params)
    
    def _on_clear(self, response):
        logging.info('clear: %s' % self)
    
    def clear(self):
        callback = self.async_callback(self._on_clear)
        self.conn.delete(callback, q='*.*')
        
    def optimize(self):
        self.conn.optimize()
        logging.info('optimize: %s' % self)
    
    def __str__(self):
        return self.conf.url
    
class SolrCoreBackend(SolrBackend):
    def __init__(self, conf, core):
        self.core = core
        super(SolrBackend, self).__init__(conf)
    
    def init(self):
        self.conn = AsyncSolr(self.conf.url+"/"+self.core)
    
    def __str__(self):
        return self.conf.url+"/"+self.core
    
class SolrCoreFactory(object):
    """
    solr = SolrCoreFactory({'url':'http://127.0.0.1:8963/solr','cores':['book','document'],'pools':2})
    solr.book.add({})
    solr.book.search({})
    solr.document.search({})
    """
    def __init__(self, conf):
        self.conf = conf
        self.backend = None
        self.engine = 'Solr'
        self.init()
    
    def init(self):
        cores = self.conf.cores
        if cores:
            admin = SolrCoreAdmin()
            for core in cores:
                ec = SolrCoreBackend(self.conf, core)
                setattr(self, core, ec)
                admin.create_core(core)
        else:
            ec = SolrBackend(self.conf)
            setattr(self, 'solr_default', ec)
        
    def __getattr__(self, key): 
        try:
            return self[key]
        except KeyError, k:
            return self['solr_default']

"""
used to create instance of SolrCoreFactory, because of the settings'name for search is "search"
then it make the proxy's name "search_proxy"
"""
search_proxy = SolrCoreFactory