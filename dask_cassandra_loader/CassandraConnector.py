import pandas as pd
from cassandra.cluster import Cluster
from cassandra.protocol import NumpyProtocolHandler

class CassandraConnector(object):
    def __init__(self, cassandra_clusters, cassandra_keyspace):
        self.error = None
        self.clusters = cassandra_clusters
        self.keyspace = cassandra_keyspace

        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)

        # Connect to Cassandra
        print("connecting to:" + str(self.clusters) + ".\n")
        self.cluster = Cluster(self.clusters)
        self.session = self.cluster.connect(self.keyspace)

        # Configure session to return a Pandas dataframe
        self.session.client_protocol_handler = NumpyProtocolHandler
        self.session.row_factory = pandas_factory

        # Tables
        self.tables = dict()
        return

    def shutdown(self):
        self.session.shutdown()
        return
