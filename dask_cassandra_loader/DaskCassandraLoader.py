from dask_cassandra_loader.CassandraConnector import CassandraConnector
from dask_cassandra_loader.CassandraLoadingQuery import CassandraLoadingQuery
from dask_cassandra_loader.CassandraTable import CassandraTable

import logging
from dask.distributed import Client, LocalCluster


class DaskCassandraLoader(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.error = None
        self.warning = None
        self.keyspace_tables = {}
        self.cassandra_con = None
        self.dask_client = None
        return

    def connect_to_local_dask(self):
        self.logger.info('Create and connect to a local Dask cluster.')
        cluster = LocalCluster()  # kwargs={'local-directory':'/home/jupyter/'})
        self.dask_client = Client(cluster, processes=False)
        return

    def disconnect_from_dask(self):
        self.dask_client.close()
        return

    def connect_to_cassandra(self, cassandra_keyspace, cassandra_clusters):
        if cassandra_keyspace == "":
            raise Exception("Key space can't be an empty string!!!")
        try:
            self.cassandra_con = CassandraConnector(cassandra_clusters, cassandra_keyspace)
        except Exception as e:
            raise Exception("It was not possible to set a connection with the Cassandra cluster: " + e)
        return

    def disconnect_from_cassandra(self):
        if self.cassandra_con is not None:
            self.cassandra_con.shutdown()
        return

    def load_cassandra_table(self, table_name, projections, and_predicates, partitions_to_eliminate):
        if table_name in self.keyspace_tables.keys():
            raise Exception(
                "Table " + table_name + " was already loaded!!!\n To reloaded it, you must first unload it.")

        table = CassandraTable(self.cassandra_con.keyspace, table_name)

        table.load_metadata(self.cassandra_con.session)
        if table.error:
            raise table.error

        loading_query = CassandraLoadingQuery()
        loading_query.set_projections(table, projections)
        if loading_query.error:
            raise loading_query.error

        loading_query.set_and_predicates(table, and_predicates)
        if loading_query.error:
            raise loading_query.error

        loading_query.partition_elimination(table, partitions_to_eliminate)
        if loading_query.error:
            raise loading_query.error

        loading_query.build_query(table)
        if loading_query.error:
            raise loading_query.error

        loading_query.print_query()

        table.load_data(self.cassandra_con.session, loading_query)
        self.keyspace_tables[table_name] = table
        return

    def unload_cassandra_table(self, table_name):
        del self.keyspace_tables[table_name]
        return
