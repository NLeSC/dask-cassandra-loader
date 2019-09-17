from dask_cassandra_loader import PagedResultHandler
import dask.dataframe as dd
import dask
import pandas as pd
from sqlalchemy import sql
from sqlalchemy.sql import text

import copy


class CassandraTable():
    """ It stores and manages metadata and data from a Cassandra table loaded into a Dask DataFrame."""

    def __init__(self, keyspace, name):
        """
        Initialization of a CassandraTable.
        > table = CassandraTable('test', 'tab1')
        
        :param keyspace: It is a string which contains an existent Cassandra keyspace.
        :param name: It is a String.
        """
        self.error = None
        self.warning = None
        self.keyspace = keyspace
        self.name = name
        self.cols = None
        self.partition_cols = None
        self.partition_keys = None
        self.predicate_cols = None

        # loading query
        self.loading_query = None
        self.data = None
        return

    def load_metadata(self, cassandra_connection):
        """
        It loads metadata from a Cassandra Table. It loads the columns names, partition columns, 
        and partition columns keys.
        > load_metadata( cassandra_con)
        
        :param cassandra_connection: It is an instance from a CassandraConnector 
        :return: 
        """
        self.cols = list(cassandra_connection.session.cluster.metadata.keyspaces[self.keyspace].tables[self.name].columns.keys())
        self.partition_cols = [f.name for f in cassandra_connection.session.cluster.metadata.keyspaces[self.keyspace].tables[
                                                   self.name].partition_key[:]]

        # load partition keys
        sql_query = sql.select([text(f) for f in self.partition_cols]).distinct().select_from(text(self.name))
        future = cassandra_connection.session.execute_async(str(sql_query))
        handler = PagedResultHandler(future)
        handler.finished_event.wait()

        if handler.error:
            raise handler.error
        else:
            self.partition_keys = handler.df

        # Create dictionary for columns which are not partition columns.
        self.predicate_cols = dict.fromkeys([f for f in self.cols if f not in list(self.partition_cols)])
        for col in self.cols:
            self.predicate_cols[col] = sql.expression.column(col)
        return

    def print_metadata(self):
        """
        It prints the metadata of a CassandraTable.
        > print_metadata()
        
        :return: 
        """
        print("The table columns are:" + str(self.table_cols))
        print("The partition columns are:" + str(self.partition_cols))
        return

    @staticmethod
    def __read_data(sql_query, clusters, keyspace):
        """
        It sets a connection with a Cassandra Cluster and loads a partition from a Cassandra table using a SQL statement.
        > __read_data( 
            'SELECT id, year, month, day from tab1 where month<1 and day in (1,2,3,8,12,30) and id=1 and year=2019',
            ['10.0.1.1', '10.0.1.2'],
            'test' )
        
        :param sql_query: A SQL query as string. 
        :param clusters: It is a list of IPs with each IP represented as a string.
        :param keyspace: It is a string which contains an existent Cassandra keyspace.
        :return: 
        """
        from cassandra.cluster import Cluster
        from cassandra.protocol import NumpyProtocolHandler

        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)

        # Set connection to a Cassandra Cluster
        cluster = Cluster(clusters)
        session = cluster.connect(keyspace)

        # Configure session to return a Pandas dataframe
        session.client_protocol_handler = NumpyProtocolHandler
        session.row_factory = pandas_factory

        # Query Cassandra
        future = session.execute_async(sql_query)
        handler = PagedResultHandler(future)
        handler.finished_event.wait()
        if handler.error:
            raise handler.error
        else:
            df = handler.df

        # Shutdown session
        session.shutdown()
        return df

    def load_data(self, cassandra_connection, caLoadingQuery):
        """
        It defines a set of SQL queries to load partitions of a Cassandra table in parallel into a Dask DataFrame.
        > load_data( cassandra_con, caLoadingQuery) 
        
        :param cassandra_connection: Instance of CassandraConnector. 
        :param caLoadingQuery: Instance of CassandraLoadingQuery.
        :return: 
        """
        futures = []

        if self.cols is None:
            self.load_metadata(self.name, cassandra_connection)

        # Reset the table's query
        self.loading_query = caLoadingQuery

        # Schedule the reads
        partition_keys = self.partition_keys.to_numpy()
        for key_values in partition_keys:
            print(key_values)
            sql_query = copy.deepcopy(self.loading_query.sql_query)
            sql_query.append_whereclause(
                text(' and '.join('%s=%s' % t for t in zip(self.partition_cols, key_values)) + ' ALLOW FILTERING'))
            query = str(sql_query.compile(compile_kwargs={"literal_binds": True}))
            future = dask.delayed(self.read_data_)(query, cassandra_connection.session.cluster.contact_points, self.keyspace)
            futures.append(future)

        # Collect results
        df = dd.from_delayed(futures)
        self.data = df.compute()
        return
