#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for the dask_cassandra_loader module.
"""
import unittest
import pytest
import pandas as pd

from cassandra.cluster import Cluster
from cassandra.protocol import NumpyProtocolHandler
from cassandra.auth import PlainTextAuthProvider
from dask_cassandra_loader import PagedResultHandler, Loader
from dask.distributed import Client, LocalCluster


def test_cassandra_connection():
    auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
    keyspace = 'dev'
    cluster = ['127.0.0.1']

    # Connect to Cassandra and create a session
    cluster = Cluster(cluster, auth_provider=auth)
    session = cluster.connect(keyspace)

    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

    session.client_protocol_handler = NumpyProtocolHandler
    session.row_factory = pandas_factory

    sql_query = 'SELECT title from play WHERE code = 1'
    table_df = None

    # Eexecute an asynchronous query
    try:
        future = session.execute_async(str(sql_query))
        handler = PagedResultHandler(future)
        handler.finished_event.wait()
    except Exception as e:
        raise AssertionError("The __read_data failed: " + str(e))
    else:
        if handler.error:
            raise Exception("The __read_data failed: " + str(handler.error))
        else:
            table_df = handler.df

    # Inspect the query result
    if table_df is None:
        raise AssertionError("No dataframe returned")
    elif table_df.empty:
        session.shutdown()
        cluster.shutdown()
        raise AssertionError()
    else:
        if table_df['title'][0] == "hello!":
            print("It works!!!")
        else:
            session.shutdown()
            cluster.shutdown()
            raise AssertionError()

    # Shutdown connection with the Cassandra Cluster
    session.shutdown()
    cluster.shutdown()
    return


def test_dask_connection():
    cluster = LocalCluster(
        scheduler_port=0,
        silence_logs=True,
        processes=False,
        asynchronous=False,
    )
    client = Client(cluster, asynchronous=False)

    def square(x):
        return x**2

    def neg(x):
        return -x

    # Run a computation on Dask
    a = client.map(square, range(10))
    b = client.map(neg, a)
    total = client.submit(sum, b)
    result = total.result()

    if result != -285:
        raise AssertionError("Result is " + str(result))
    else:
        print("The result is correct!!!")

    client.close()
    cluster.close()
    return True


def test_table_load_empty():
    keyspace = 'dev'
    cluster = ['127.0.0.1']

    # Connect to Cassandra
    dask_cassandra_loader = Loader()
    dask_cassandra_loader.connect_to_cassandra(cluster,
                                               keyspace,
                                               username='cassandra',
                                               password='cassandra')

    # Connect to Dask
    dask_cassandra_loader.connect_to_local_dask()

    # Load table 'tab1'
    table = dask_cassandra_loader.load_cassandra_table(
        'tab1', ['id', 'year', 'month', 'day'],
        [('month', 'less_than', [1]),
         ('day', 'in_', [1, 2, 3, 8, 12, 30])], [('id', [1, 2, 3, 4, 5, 6]),
                                                 ('year', [2019])],
        force=False)

    if table is None:
        raise AssertionError()

    if table.data is not None:
        raise AssertionError("Table.data is supposed to be None!!!")
    else:
        print("As expected table data is empty!!!")

    # Disconnect from Dask
    dask_cassandra_loader.disconnect_from_dask()

    # Disconnect from Cassandra
    dask_cassandra_loader.disconnect_from_cassandra()
    return


def test_table_load_with_data():
    keyspace = 'dev'
    cluster = ['127.0.0.1']

    # Connect to Cassandra
    dask_cassandra_loader = Loader()
    dask_cassandra_loader.connect_to_cassandra(cluster,
                                               keyspace,
                                               username='cassandra',
                                               password='cassandra')

    # Connect to Dask
    dask_cassandra_loader.connect_to_local_dask()
    # Load table 'tab1'
    table = dask_cassandra_loader.load_cassandra_table('tab1',
                                               ['id', 'year', 'month', 'day'],
                                               [('day', 'equal', [8])],
                                               [('id', [18]), ('year', [2018]),
                                                ('month', [11])],
                                               force=False)

    if table is None:
        raise AssertionError("Table is not supposed to be None!!!")

    if table.data is None:
        raise AssertionError("Table.data is not supposed to be None!!!")

    # Inspect table information
    #print(table.data.head())

    # Disconnect from Dask
    dask_cassandra_loader.disconnect_from_dask()

    # Disconnect from Cassandra
    dask_cassandra_loader.disconnect_from_cassandra()
    return


def test_with_error():
    with pytest.raises(ValueError):
        # Do something that raises a ValueError
        raise (ValueError)


@pytest.fixture
def an_object():
    return {}


def test_dask_cassandra_loader(an_object):
    if an_object != {}:
        raise AssertionError()
