# -*- coding: utf-8 -*-

import logging

from dask_cassandra_loader.__version__ import __version__

logging.getLogger(__name__).addHandler(logging.NullHandler())

__author__ = "Romulo Goncalves"
__email__ = 'r.goncalves@esciencecenter.nl'

from dask_cassandra_loader.PagedResultHandler import PagedResultHandler
from dask_cassandra_loader.CassandraOperators import CassandraOperators
from dask_cassandra_loader.CassandraConnector import CassandraConnector
from dask_cassandra_loader.CassandraLoadingQuery import CassandraLoadingQuery
from dask_cassandra_loader.CassandraTable import CassandraTable

import logging


logger = logging.getLogger('cerulean')
"""The Dask Cassandra Loader root logger. Use this to set Dask Cassandra Loader's log level.

In particular, if something goes wrong and you want more debug output, you \
can do::
    import logging

    dask_cassandra_loader.logger.setLevel(logging.INFO)

or for even more::

    dask_cassandra_loader.logger.setLevel(logging.DEBUG)
"""

__all__ = ['PagedResultHandler', 'CassandraOperators', 'CassandraConnector', 'CassandraLoadingQuery', 'CassandraLoadingQuery', 'CassandraTable']
