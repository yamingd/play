# -*- coding: utf-8 -*-
#!/usr/bin/env python
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""A lightweight wrapper around MySQLdb."""
import logging
log = logging.getLogger(__name__)

import copy
import MySQLdb.constants
import MySQLdb.converters
import MySQLdb.cursors
import itertools
import time

from Queue import Queue
from contextlib import contextmanager

from play.db import Row

class Connection(object):
    """A lightweight wrapper around MySQLdb DB-API connections.

    The main value we provide is wrapping rows in a dict/object so that
    columns can be accessed by name. Typical usage:

        db = database.Connection("localhost", "mydatabase")
        for article in db.query("SELECT * FROM articles"):
            print article.title

    Cursors are hidden by the implementation, but other than that, the methods
    are very similar to the DB-API.

    We explicitly set the timezone to UTC and the character encoding to
    UTF-8 on all connections to avoid time zone and encoding errors.
    """
    def __init__(self, host, database, user=None, password=None,
                 max_idle_time=7*3600, debug=False):
        self.host = host
        self.database = database
        self.max_idle_time = max_idle_time
        self.debug = debug
        args = dict(conv=CONVERSIONS, use_unicode=True, charset="utf8",
                    db=database, init_command='SET time_zone = "+0:00"',
                    sql_mode="ANSI,STRICT_TRANS_TABLES,TRADITIONAL")
        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        # We accept a path to a MySQL socket file or a host(:port) string
        if "/" in host:
            args["unix_socket"] = host
        else:
            self.socket = None
            pair = host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = host
                args["port"] = 3306

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except:
            log.error("Cannot connect to MySQL on %s", self.host,
                          exc_info=True)

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = MySQLdb.connect(**self._db_args)
        self._db.autocommit(True)

    def iter(self, query, *parameters, **kwargs):
        """Returns an iterator for the given query and parameters."""
        clz = kwargs.get('clz',Row)
        self._ensure_connected()
        cursor = MySQLdb.cursors.SSCursor(self._db)
        try:
            self._execute(cursor, query, parameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield clz(zip(column_names, row))
        except:
            log.exception('unexpected error')
            raise
        finally:
            cursor.close()

    def query(self, query, *parameters, **kwargs):
        """Returns a row list for the given query and parameters."""
        clz = kwargs.get('clz',Row)
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            column_names = [d[0] for d in cursor.description]
            return [clz(itertools.izip(column_names, row)) for row in cursor]
        except:
            log.exception('unexpected error')
            raise
        finally:
            cursor.close()

    def get(self, query, *parameters, **kwargs):
        """Returns the first row returned for the given query."""
        rows = self.query(query, *parameters, **kwargs)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]
    
    def show(self, table_name):
        query = 'SHOW COLUMNS FROM %s' % table_name
        cursor = self._cursor()
        try:
            self._execute(cursor, None, None)
            column_names = [d[0] for d in cursor.description]
            return [Row(itertools.izip(column_names, row)) for row in cursor]
        except:
            log.exception('unexpected error, show table='+table_name)
            raise
        finally:
            cursor.close()
    
    def execute(self, query, *parameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if (self._db is None or
            (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def _execute(self, cursor, query, parameters):
        try:
            try:
                if self.debug:
                    log.debug(query)
                    log.debug(parameters)
            except:
                pass
            return cursor.execute(query, parameters)
        except OperationalError:
            log.error("Error connecting to MySQL on %s", self.host)
            self.close()
            raise

# Fix the access conversions to properly recognize unicode/binary
FIELD_TYPE = MySQLdb.constants.FIELD_TYPE
FLAG = MySQLdb.constants.FLAG
CONVERSIONS = copy.deepcopy(MySQLdb.converters.conversions)

field_types = [FIELD_TYPE.BLOB, FIELD_TYPE.STRING, FIELD_TYPE.VAR_STRING]
if 'VARCHAR' in vars(FIELD_TYPE):
    field_types.append(FIELD_TYPE.VARCHAR)

for field_type in field_types:
    CONVERSIONS[field_type].insert(0, (FLAG.BINARY, str))


# Alias some common MySQL exceptions
IntegrityError = MySQLdb.IntegrityError
OperationalError = MySQLdb.OperationalError

class ConnectionPool(Queue):
    def __init__(self, rc=None, n_slots=5):
        Queue.__init__(self, n_slots)
        if rc is not None:
            self.fill(rc, n_slots)

    @contextmanager
    def reserve(self, timeout=None):
        """Context manager for reserving a client from the pool.
        If *timeout* is given, it specifiecs how long to wait for a client to
        become available.
        """
        rc = self.get(True, timeout=timeout)
        try:
            yield rc
        finally:
            self.put(rc)

    def fill(self, rc, n_slots):
        """Fill *n_slots* of the pool with clones of *mc*."""
        for i in xrange(n_slots):
            self.put(rc.clone())

class ConnectionProxy(object):
    def __init__(self, conf):
        self.host = conf.host
        self.database = conf.dbname
        self.clients = ConnectionPool(n_slots = conf.pools)
        for x in xrange(conf.pools):
            r = Connection(self.host, self.database, user=conf.user, password=conf.passwd, debug=conf.debug)
            self.clients.put(r)
        log.info('init MySQL Connection Proxy')
        
def instrument(name):
    def do(self, *args, **kwargs):
        try:
            with self.clients.reserve() as rc:
                return getattr(rc, name)(*args, **kwargs)
        except:
            log.exception('unexpected error:%s',args)
            raise
    return do

for meth,f in Connection.__dict__.items():
    if meth.startswith('_'):
        continue
    setattr(ConnectionProxy, meth, instrument(meth))

dbm_proxy = ConnectionProxy