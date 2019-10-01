#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the dask_cassandra_loader module.
"""
import pytest
import pandas as pd

from cassandra.cluster import Cluster
from cassandra.protocol import NumpyProtocolHandler
from cassandra.auth import PlainTextAuthProvider
from dask_cassandra_loader import PagedResultHandler
from dask_cassandra_loader.dask_cassandra_loader import DaskCassandraLoader
from cassandra.policies import RoundRobinPolicy


def test_cassandra_connection():
    auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
    keyspace = 'dev'
    clusters = ['127.0.0.1']

    cluster = Cluster(clusters, auth_provider=auth, load_balancing_policy=RoundRobinPolicy())
    session = cluster.connect(keyspace)

    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

    session.client_protocol_handler = NumpyProtocolHandler
    session.row_factory = pandas_factory

    sql_query = 'SELECT title from play WHERE code = 1'

    future = session.execute_async(str(sql_query))
    handler = PagedResultHandler(future)
    handler.finished_event.wait()

    table_df = handler.df

    session.shutdown()
    if table_df.empty:
        raise AssertionError()
    else:
        if table_df['title'][0] == "hello!":
            print("It works!!!")
        else:
            raise AssertionError()

    return


def test_dask_connection():
    dask_cassandra_con = DaskCassandraLoader()

    dask_cassandra_con.connect_to_local_dask()

    def square(x):
        return x ** 2

    def neg(x):
        return -x

    A = dask_cassandra_con.dask_client.map(square, range(10))
    B = dask_cassandra_con.dask_client.map(neg, A)
    total = dask_cassandra_con.dask_client.submit(sum, B)

    dask_cassandra_con.disconnect_from_Dask()

    if total.result() != -285:
        raise AssertionError()


def test_table_load():
    keyspace = 'dev'
    clusters = ['127.0.0.1']

    # Connect to Cassandra
    dask_cassandra_loader = DaskCassandraLoader()
    dask_cassandra_loader.connect_to_cassandra(keyspace, clusters, username='cassandra', password='cassandra')

    # Connect to Dask
    dask_cassandra_loader.connect_to_local_dask()

    # Load table 'tab1'
    dask_cassandra_loader.load_cassandra_table('tab1',
                         ['id', 'year', 'month', 'day'],
                         [('month', 'less_than', 1), ('day', 'in_', [1, 2, 3, 8, 12, 30])],
                         [(id, [1, 2, 3, 4, 5, 6]), ('year', [2019])])
    table = dask_cassandra_loader.keyspace_tables['tab1']

    # Disconnect from Dask
    dask_cassandra_loader.disconnect_from_dask()

    # Disconnect from Cassandra
    dask_cassandra_loader.disconnect_from_cassandra()

    # Inspect table information
    table.data.info()
    print(table.data.head())


def test_with_error():
    with pytest.raises(ValueError):
        # Do something that raises a ValueError
        raise(ValueError)


# Fixture example
@pytest.fixture
def an_object():
    return {}


def test_dask_cassandra_loader(an_object):
    if an_object != {}:
        raise AssertionError()
