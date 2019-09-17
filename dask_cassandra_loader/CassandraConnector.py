import pandas as pd
from cassandra.cluster import Cluster
from cassandra.protocol import NumpyProtocolHandler


class CassandraConnector(object):
    """ It sets and manages a connection to a Cassandra Cluster. """

    def __init__(self, cassandra_clusters, cassandra_keyspace):
        """
        Initialization of CassandraConnector. It connects to a Cassandra cluster defined by a list of IPs.
        If the connection is successful, it then establishes a session with a Cassandra keyspace.
        > CassandraConnector(['10.0.1.1', '10.0.1.2'], 'test')
        
        :param cassandra_clusters: It is a list of IPs with each IP represented as a string. 
        :param cassandra_keyspace: It is a string which contains an existent Cassandra keyspace.
        """
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
        """
        Shutdowns the existing connection with a Cassandra cluster.
        > shutdown()
        
        :return: 
        """
        self.session.shutdown()
        return
