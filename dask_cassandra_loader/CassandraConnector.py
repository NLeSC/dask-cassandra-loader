import pandas as pd
from cassandra.cluster import Cluster
from cassandra.protocol import NumpyProtocolHandler
from cassandra.auth import PlainTextAuthProvider


class CassandraConnector(object):
    """ It sets and manages a connection to a Cassandra Cluster. """

    def __init__(self, cassandra_clusters, cassandra_keyspace, username, password):
        """
        Initialization of CassandraConnector. It connects to a Cassandra cluster defined by a list of IPs.
        If the connection is successful, it then establishes a session with a Cassandra keyspace.
        > CassandraConnector(['10.0.1.1', '10.0.1.2'], 'test')
        :param cassandra_clusters: It is a list of IPs with each IP represented as a string.
        :param cassandra_keyspace: It is a string which contains an existent Cassandra keyspace.
        :param username: It is a String.
        :param password: It is a String.
        """
        self.error = None
        self.clusters = cassandra_clusters
        self.keyspace = cassandra_keyspace
        self.auth = None

        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)

        # Connect to Cassandra
        print("connecting to:" + str(self.clusters) + ".\n")
        if username is None:
            self.cluster = Cluster(self.clusters)
        else:
            self.auth = PlainTextAuthProvider(username=username, password=password)
            self.cluster = Cluster(self.clusters, auth_provider=self.auth)
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
        self.cluster.shutdown()
        return
