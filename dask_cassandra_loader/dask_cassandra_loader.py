from dask_cassandra_loader.CassandraConnector import CassandraConnector
from dask_cassandra_loader.CassandraLoadingQuery import CassandraLoadingQuery
from dask_cassandra_loader.CassandraTable import CassandraTable

import logging
from dask.distributed import Client, LocalCluster


class DaskCassandraLoader(object):
    """  A loader to populate a Dask Dataframe with data from a Cassandra table. """

    def __init__(self):
        """ 
        Initialization of DaskCassandraLoader
        > DaskCassandraLoader()
        
        """
        self.logger = logging.getLogger(__name__)
        self.error = None
        self.warning = None
        self.keyspace_tables = {}
        self.cassandra_con = None
        self.dask_client = None
        return

    def connect_to_local_dask(self):
        """
        Connects to a local Dask cluster.
        > connect_to_local_dask()
        
        :return: 
        """
        self.logger.info('Create and connect to a local Dask cluster.')
        cluster = LocalCluster()  # kwargs={'local-directory':'/home/jupyter/'})
        self.dask_client = Client(cluster, processes=False)
        return

    def disconnect_from_dask(self):
        """
        Ends the established Dask connection.
        > disconnect_from_dask()
        
        :return: 
        """
        self.dask_client.close()
        return

    def connect_to_cassandra(self, cassandra_keyspace, cassandra_clusters):
        """
        Connects to a Cassandra cluster specified by a list of IPs.
        > connect_to_cassandra('test', ['10.0.1.1', '10.0.1.2'])
        
        :param cassandra_keyspace: It is a string which contains an existent Cassandra keyspace.
        :param cassandra_clusters: It is a list of IPs with each IP represented as a string.
        :return: 
        """
        if cassandra_keyspace == "":
            raise Exception("Key space can't be an empty string!!!")
        try:
            self.cassandra_con = CassandraConnector(cassandra_clusters, cassandra_keyspace)
        except Exception as e:
            raise Exception("It was not possible to set a connection with the Cassandra cluster: " + e)
        return

    def disconnect_from_cassandra(self):
        """
        Ends the established Cassandra connection.
        > disconnect_from_cassandra()
        
        :return: 
        """
        if self.cassandra_con is not None:
            self.cassandra_con.shutdown()
        return

    def load_cassandra_table(self, table_name, projections, and_predicates, partitions_to_load):
        """
        It loads a Cassandra table into a Dask dataframe.
        > load_cassandra_table('tab1', 
                ['id', 'year', 'month', 'day'], 
                [('month', 'less_than', 1), ('day', 'in_', [1,2,3,8,12,30])],
                 [(id, [1, 2, 3, 4, 5, 6]), ('year',[2019])] )
                 
        :param table_name: It is a String. 
        :param projections: A list of columns names. Each column name is a String.
        :param and_predicates: List of triples. Each triple contains column name as String,
        operator name as String, and a list of values depending on the operator. CassandraOperators.print_operators()
        prints all available operators. It should only contain columns which are not partition columns.
        :param partitions_to_load: List of tuples. Each tuple as a column name as String
        and a list of keys which should be selected. It should only contain columns which are partition columns.
        :return: 
        """
        if table_name in self.keyspace_tables.keys():
            raise Exception(
                "Table " + table_name + " was already loaded!!!\n To reloaded it, you must first unload it.")

        table = CassandraTable(self.cassandra_con.keyspace, table_name)

        table.load_metadata(self.cassandra_con)
        if table.error:
            raise table.error

        loading_query = CassandraLoadingQuery()
        loading_query.set_projections(table, projections)
        if loading_query.error:
            raise loading_query.error

        loading_query.set_and_predicates(table, and_predicates)
        if loading_query.error:
            raise loading_query.error

        loading_query.partition_elimination(table, partitions_to_load)
        if loading_query.error:
            raise loading_query.error

        loading_query.build_query(table)
        if loading_query.error:
            raise loading_query.error

        loading_query.print_query()

        table.load_data(self.cassandra_con, loading_query)
        self.keyspace_tables[table_name] = table
        return

    def unload_cassandra_table(self, table_name):
        """
        Deletes the DaskDataframe and removes the table from the list of 'loaded' tables.
        
        :param table_name: It is a String. 
        :return: 
        """
        del self.keyspace_tables[table_name].data
        del self.keyspace_tables[table_name]
        return
