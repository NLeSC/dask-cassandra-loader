========
Tutorial
========

Welcome to the Dask cassandra loader tutorial. This tutorial demonstrates the basics of using
Dask cassandra loader using either a local or a remote Cassandra database.

To install Dask cassandra loader, use

.. code-block:: bash

  pip install dask_cassandra_loader

If you're using dask cassandra loader in a program, you will probably want to use a
virtualenv and install Cerulean into that, together with your other
dependencies.

Setup
=====

The tutorial requires the creation of a keyspace in an existent Cassandra cluster. For this
tutorial it is used the keyspace called **tutorial**. In this example it is assume the local
client cqlsh is installed and configured accordingly.

.. code-block:: bash

    cqlsh -e "create keyspace tutorial with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

Once the keyspace is created the user needs to create a table and load it. To do that the
user needs to run the :download:`tutorial.cql <tutorial.cql>` file as follow:

.. code-block:: bash

    cqlsh --keyspace=tutorial -f tutorial.cql

Once the table is loaded, the user will have a table called **tab1** with the following schema: 

.. code-block:: sql

  create table tab1(id int, year int, month int, day int, timest timestamp, lat float, lon float, PRIMARY KEY((id, year, month)));
  
The loaded data has two partitions due two distinct months.


Dask cassandra loader
=====================

The first step to load a table from Cassandra into a Dask data-frame is to create :class:`dask_cassandra_loader.loader.Loader`.
To do that the user should do the following:

.. code-block:: python

  from dask_cassandra_loader import Loader
  
  dask_cassandra_loader = Loader()


Connect to Cassandra
--------------------

With the loader the user is then able to set a connection to an existent Cassandra cluster.
In this example we assume the user is connecting to local cluster using the default credentials.

.. code-block:: python

  keyspace = 'tutorial'
  cluster = ['127.0.0.1']

  dask_cassandra_loader.connect_to_cassandra(cluster,
                                             keyspace,
                                             username='cassandra',
                                             password='cassandra')


Connect to Dask
---------------

Before a table is loaded it is necessary to connect to a Dask Cluster. For testing proposes
it might be handy to have the option to create a **LocalCluster**. Both options are supported as
the following examples will show.

To create and connect to a local Dask cluster you use the following code:

.. code-block:: python

  dask_cassandra_loader.connect_to_local_dask()

To connect to a remote cluster you use the following code:

.. code-block:: python

  cluster = "host1.domain.nl:9091"
  dask_cassandra_loader.connect_to_dask(cluster):


Read Table
----------

In this example the user will load table *tab1*, project columns *id*, *year*, *month*, *day*,
have a predicate on column *day* (*day = 18*) and only select the partitions for which *id in [18]*,
*year in [2018]* and *month in [11]*. In this example, it is requested to not load all partitions in
case the query qualifies all of them for loading. For more details about the function, the user should
read :func:`dask_cassandra_loader.loader.Loader.load_cassandra_table`.

.. code-block:: python

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
  print(table.data.head())


More information
================

To find all the details of what dask cassandra loader can do and how to do it, please refer
to the :doc:`API documentation<apidocs/dask_cassandra_loader.loader>`.
