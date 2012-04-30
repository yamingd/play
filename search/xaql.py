# -*- coding: utf-8 -*-
import logging
log = logging.getLogger(__name__)

import cPickle
import string
import bisect

import xapian

from xapian import (
    QueryParser, Query, Stem, Enquire, MSET_DID, MSET_PERCENT,
    ExpandDecider, RSet)

from mmseg import seg_txt
from mmseg.search import seg_txt_search,seg_txt_2_dict

from base import BaseIndexReader, BaseIndexWriter
from tube.lib.encoding import _force_utf8,_force_unicode

class lazy_property(object):
    def __init__(self, fn):
        self.fn = fn
        self.__name__ = fn.func_name
        self.__doc__ = fn.__doc__

    def __get__(self, obj, cls):
        if obj is None:
            return None
        obj.__dict__[self.__name__] = result = self.fn(obj)
        return result


class Record(object):
    """
    An object returned by a select query by Xapian to represent the
    matched document.  It's attributes match the names and values of
    the data selected out of the matching document's pickled document
    data storage.  Since all 
    """

    def __init__(self, attrs):
        for name, value in attrs.items():
            if isinstance(value, str):
                value = value.decode('utf8')
            setattr(self, name, value)


class Decider(ExpandDecider):
    """
    A Xapian ExpandDecider that decide which terms to keep and which
    to discard when expanding a query using the "suggest" syntax.  As
    a place to start, we throw out:

      - Terms that don't begin with an uppercase letter or digit.
        This filters prefixed terms and stemmed forms.

      - Terms shorter than 4 chars, which are likely irrelevant

      - Stopwords for the given language.  Default is english, pass
        None for the lang argument if no stopping is desired.
    """

    nostart = unicode(string.uppercase+string.digits)

    def __init__(self):
        super(Decider, self).__init__()
        self.stopper = lambda(term): False

    def __call__(self, term):
        if term[0] in self.nostart or len(term) < 3: # or self.stopper(term):
            return False
        return True

DOC_ITEM_ID = 1
DOC_ITEM_TYPE = 0
TERM_MIN_LENGTH = 1

def gen_terms(cont):
    if cont is None:
        return []
    cont = cont.strip()
    if len(cont)==0:
        return []
    if len(cont)<TERM_MIN_LENGTH:
        return []
    terms = [item for item in seg_txt(cont) if len(item)>TERM_MIN_LENGTH]
    if len(cont)<10:
        terms.append(cont)
    terms = list(set(terms))
    return terms
    
class XaqlWriter(BaseIndexWriter):
    
    _flags = xapian.DB_CREATE_OR_OPEN    

    def __init__(self, path, overwrite=False):
        if overwrite:
            self._flags = xapian.DB_CREATE_OR_OVERWRITE
        self._path = path
        self.writer = xapian.WritableDatabase(self._path, self._flags)
    
    def __del__(self):
        if self.writer:
            self.writer.close()
    
    def _doc_key(self,item_id,item_clz):
        return '%s:%s' % (item_clz.__name__,item_id)
    
    def save(self, entity):
        """
        mq任务调用,
        """
        m = entity.gen_index_doc()
        dockey = self._doc_key(m['item_id'],entity.__class__)
        doc = xapian.Document()
        terms = m.get('terms',[])
        texts = m.get('text',[])
        for text in texts:
            terms.extend(gen_terms(text))
        terms = filter(None,list(set(terms)))
        for term in terms:
            doc.add_term(_force_utf8(term.lower()))
        doc.add_term('M:'+entity.__class__.__name__)
        doc.add_term('I:'+str(entity.id))
        doc.add_value(DOC_ITEM_ID,xapian.sortable_serialise(m['item_id']))
        doc.add_value(DOC_ITEM_TYPE,entity.__class__.__name__)
        self.writer.replace_document(dockey, doc)

    def remove(self, entity):
        self.writer.delete_document(self._doc_key(entity.id,entity.__class__))

    def begin(self):
        self.writer.begin_transaction()

    def rollback(self):
        self.writer.cancel_transaction()

    def commit(self):
        self.writer.commit_transaction()

    def flush(self):
        self.writer.flush()


parser_flags = (QueryParser.FLAG_PHRASE | QueryParser.FLAG_BOOLEAN |
                QueryParser.FLAG_LOVEHATE | QueryParser.FLAG_SPELLING_CORRECTION |
                QueryParser.FLAG_BOOLEAN_ANY_CASE | QueryParser.FLAG_WILDCARD)

class XaqlReader(BaseIndexReader):
    """ Xapian query interface and query language parser.

    For now i've taken out the pyparsing parser bits.  Those will be
    reincarnated soon.  ATM I'd like to focus on the core programatic
    functionality.
    """
    def __init__(self, database_path, stem=QueryParser.STEM_SOME,
                 prefixes=None,sort_names=None):
        """Construct a read-only xaql object for the Xapian database 
           at 'database_path'

        'stem' - The Xapian stemming strategy.
        
        'prefixes' = A dictionary mapping prefix names to prefix codes.

        'sort_names' - A list of ordered sort names that match the
          order of sort values in xapian.
        """
        self.database = xapian.Database(database_path)
        self.prefixes = prefixes
        self.sort_names = sort_names
        self.stem = stem
    
    def _query_parser(self):
        query_parser = QueryParser()
        query_parser.set_database(self.database)
        query_parser.set_default_op(Query.OP_AND)
        query_parser.set_stemmer(Stem("none"))
        query_parser.add_boolean_prefix('model', 'M:')
        query_parser.add_boolean_prefix('modelid', 'I:')
        if self.prefixes is not None:
            for name, prefix in self.prefixes.items():
                if prefix == 'M:' or prefix == 'I:':
                    continue 
                query_parser.add_boolean_prefix(name, prefix)
        query_parser.set_stemming_strategy(self.stem)
        return query_parser
    
    def _parse_query(self,query,partial=False):
        query_parser = self._query_parser()
        if partial:
            query = query_parser.parse_query(query, (parser_flags | xapian.QueryParser.FLAG_PARTIAL))
        else:
            query = query_parser.parse_query(query, parser_flags)
        return query, query_parser
    
    def _add_model_query(self,query,model_clz):
        if model_clz:
            if not isinstance(model_clz,list):
                model_query = Query("M:" + model_clz.__name__)
            else:
                qs = []
                for clz in model_clz:
                    qs.append(Query("M:" + clz.__name__))
                model_query = Query(Query.OP_OR, qs)
            query = Query(Query.OP_AND, model_query, query)
        return query
    
    def __del__(self):
        if self.database:
            self.database.close()
            
    def _generate_records(self, mset, select=set(["*"])):
        """
        仅返回item_id,item_type,外部再从memcached、db中读取详细数据
        """
        for m in mset:
            result = {"_did" : m.docid, "_score" : m.percent, "_rank" : m.rank, "_collapse_count" : m.collapse_count, "_weight" : m.weight}
            result['item_id'] = int(xapian.sortable_unserialise(m.document.get_value(DOC_ITEM_ID))) #int
            result['item_type'] = m.document.get_value(DOC_ITEM_TYPE)  #string
            
            if select:
                doc = m.document
                data_str = doc.get_data()
                if len(data_str):
                    data_dict = cPickle.loads(data_str)
                    for key, value in data_dict.items():
                        if key in select or "*" in select:
                            result[key] = value

            yield result

    def reopen(self):
        """ Reopen the database to get latest revision. """
        self.database.reopen()

    def get_doccount(self):
        """
        Return the number of indexed documents, handy for tests and
        sanity check.
        """
        self.database.reopen()
        return self.database.get_doccount()

    def term_freq(self, term):
        """
        Return a count of the number of documents indexed for a given
        term.  Useful for testing.
        """
        self.database.reopen()
        return self.database.get_termfreq(term)

    def describe_query(self, query, partial=False):
        """
        Describe the parsed query.
        """
        query,query_parser = self._parse_query(query,partial=partial)
        return query
    
    def spell(self, query, partial=False):
        """
        Suggest a query string with corrected spelling.
        """
        self.database.reopen()
        query_parser = self._query_parser()
        flags = xapian.QueryParser.FLAG_SPELLING_CORRECTION
        if partial:
            flags = flags | xapian.QueryParser.FLAG_PARTIAL
        query_parser.parse_query(query, flags)
        return query_parser.get_corrected_query_string().decode('utf8')

    def suggest(self, model_clz, query, limit=10, offset=0, order=None, ascending=True):
        """
        Suggest terms that would possibly yield more relevant results
        for the given query.
        """
        self.database.reopen()
        enq = Enquire(self.database)
        enq.set_collapse_key(DOC_ITEM_ID)
        if order is not None:
            if issubclass(type(order), basestring):
                order = self.sort_names.index(order)
                if order == -1:
                    raise TypeError("There is no sort name %s" % order)
            enq.set_sort_by_value(order, ascending)
        else:
            enq.set_sort_by_relevance()
        
        query_parser = self._query_parser()
        query = query_parser.parse_query(query)
        query = self._add_model_query(query,model_clz)
        log.debug(query)    
        enq.set_query(query)
        mset = enq.get_mset(offset, limit)
        rset = RSet()
        for m in mset:
            rset.add_document(m[MSET_DID])
            
        eset = enq.get_eset(limit, rset)

        for item in eset.items:
            yield (item[0].decode('utf8'), item[1])

    def estimate(self, model_clz, query, limit=10, partial=False):
        """
        Estimate the number of documents that will be yielded with
        the given query.  limit tells the estimator the minimum number
        of documents to consider.
        """
        self.database.reopen()
        enq = Enquire(self.database)
        enq.set_collapse_key(DOC_ITEM_ID)
        query, query_parser = self._parse_query(query, partial=partial)
        query = self._add_model_query(query,model_clz)
        log.debug(query)    
        enq.set_query(query)
        return enq.get_mset(0, 0, limit).get_matches_estimated()
    
    def select(self, model_clz, query, select=set(["*"]), limit=10, offset=0, order=None, 
               ascending=True, partial=False):
        """Select documents from the database matching 'query'.

        'select' - The set of keys from the documents picked data
        dictionary to return or use to construct a record.  '*' means
        all available keys.

        'limit' - The number of records to return.

        'offset' - How many records to skip before returning.

        'order' - The number or name (if order_names was provided to
        the constructor) of the column to sort the results by.

        'ascending' - Whether to sort the results in ascending or
        descending order.

        'partial' - Wether to support wildcard partial queries like "foo*".
        """
        self.database.reopen()
        enq = Enquire(self.database)
        enq.set_collapse_key(DOC_ITEM_ID)
        if order is not None:
            if issubclass(type(order), basestring):
                order = self.sort_names.index(order)
                if order == -1:
                    raise TypeError("There is no sort name %s" % order)
            enq.set_sort_by_value(order, ascending)
        else:
            enq.set_sort_by_relevance()
            
        query, query_parser = self._parse_query(query, partial=partial)
        query = self._add_model_query(query,model_clz)
        log.debug(query)
        enq.set_query(query)
        mset = enq.get_mset(offset, limit)
        return self._generate_records(mset, select)

    def morelike(self, model_clz, query, select=["*"], limit=10, offset=0, order=None, 
                ascending=True):
        """ Find documents in the database most relevant to the given terms.

        'select' - The set of keys from the documents picked data
        dictionary to return or use to construct a record.  '*' means
        all available keys.

        'limit' - The number of records to return.

        'offset' - How many records to skip before returning.

        'order' - The number or name (if order_names was provided to
        the constructor) of the column to sort the results by.

        'ascending' - Whether to sort the results in ascending or
        descending order.

        """

        self.database.reopen()
        suggested_terms = self.suggest(model_clz, query, limit, offset, order, ascending)
        terms = [term[0] for term in suggested_terms]
        
        enq = Enquire(self.database)
        enq.set_collapse_key(DOC_ITEM_ID)
        if order is not None:
            if issubclass(type(order), basestring):
                order = self.sort_names.index(order)
                if order == -1:
                    raise TypeError("There is no sort name %s" % order)
            enq.set_sort_by_value(order, ascending)
        else:
            enq.set_sort_by_relevance()
                
        query = Query(Query.OP_ELITE_SET, terms, limit)
        query = self._add_model_query(query,model_clz)
        enq.set_query(query)
        mset = enq.get_mset(offset, limit)
        return self._generate_records(mset, select)


    def suggest_queries(self, query, extend=True,
                        count=1, min_term_len=4, max_term_len=15, relevance=100):
        """
        Given a query, suggest other queries.

        'query' - the original query to use.

        'extend' - whether to extend to original query or not.

        'count' - The number of terms to suggest in the suggested queries.

        'min_term_len' - The minimum size of the terms to suggest.

        'max_term_len' - The maximum size of the terms to suggest.

        'relevance' - How many base relevant terms to query from
                      xapian before filtering.
        """
        split_query = query.split()
        query_terms = set(split_query)
        word = split_query[-1]
        allterms = self.database.allterms(word)

        popular_terms = []
        for term in allterms:
            frequency = self.database.get_collection_freq(term.term)
            if len(term.term) < max_term_len:
                bisect.insort(popular_terms, (frequency, term))

        popular_terms = popular_terms[-count:]
        popular_terms.reverse()
        suggestions = []
        #stemmer = xapian.Stem('none')

        for frequency, term in popular_terms:
            #stemmed_term = stemmer(term.term)
            inner_term = ""
            inner_terms = self.suggest(term.term, limit=relevance)

            for inner_term, score in inner_terms:

                if (len(inner_term) > min_term_len and 
                    len(inner_term) < max_term_len and 
                    #stemmer(inner_term) != stemmed_term and 
                    inner_term not in query_terms):
                    break

            # now append the new term to the original query
            query = u"%s %s %s" % ((u" ".join(split_query[:-1])).strip(), 
                                   term.term, inner_term)

            estimate = self.estimate(query, limit=10000)

            if estimate == 0:
                continue

            suggestions.append((query.strip(), estimate))

        return suggestions