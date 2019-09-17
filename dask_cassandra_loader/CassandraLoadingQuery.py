from . import CassandraOperators
from sqlalchemy import sql
from sqlalchemy.sql import text


class CassandraLoadingQuery(object):
    """ Class to define a SQL select statement over a Cassandra table. """

    def __init__(self):
        """ 
        Initialization of CassandraLoadingQuery
        > CassandraLoadingQuery()
        
        """
        self.error = None
        self.warning = None
        self.projections = None
        self.and_predicates = None
        self.sql_query = None
        return

    def set_projections(self, table, projections):
        """
        It set the list of columns to be projected, i.e., selected.
        > set_projections(table, ['id', 'year', 'month', 'day'])
        
        :param table: Instance of class CassandraTable
        :param projections: A list of columns names. Each column name is a String.
        :return: 
        """
        if projections is None or len(projections) == 0:
            print("All columns will be projected!!!")
            self.projections = projections
        else:
            for col in projections:
                if col not in table.cols:
                    raise Exception("Invalid column, please use one of the following columns: " + str(table.cols)
                                    + "!!!")
            self.projections = list(dict.fromkeys(projections))
        return

    def drop_projections(self):
        """
        It drops the list of columns to be projected, i.e., selected.
        > drop_projections()

        :return: 
        """
        self.projections = None
        return

    def set_and_predicates(self, table, predicates):
        """
        It sets a list of predicates with 'and' clause over the non partition columns of a Cassandra's table. 
        > set_and_predicates(table, [('month', 'less_than', 1), ('day', 'in_', [1,2,3,8,12,30])])
        
        :param table: Instance of class CassandraTable.
        :param predicates: List of triples. Each triple contains column name as String,
        operator name as String, and a list of values depending on the operator. CassandraOperators.print_operators()
        prints all available operators. It should only contain columns which are not partition columns.
        :return: 
        """
        if predicates is None or len(predicates) == 0:
            print("No predicates over the non primary key columns were defined!!!")
        else:
            operators = CassandraOperators()
            for predicate in predicates:
                (col, op, values) = predicate
                if col not in table.predicate_cols:
                    raise Exception("Predicate: " + str(predicate)
                                    + " has an primary key column. Pick a non-primary key column "
                                    + str(table.predicate_cols.keys() + "!!!\n"))
                else:
                    self.predicates.append(operators.create_predicate(table, col, op, values))
        return

    def remove_and_predicates(self):
        """
        It drops the list of predicates with 'and' clause over the non partition columns of a Cassandra's table.
        > remove_and_predicates()
        
        :return: 
        """
        self.and_predicates = None
        return

    @staticmethod
    def partition_elimination(table, partitions_to_load, force):
        """
        It does partition elimination when by selecting only a range of partition key values.
        > partition_elimination( table, [(id, [1, 2, 3, 4, 5, 6]), ('year',[2019])] )
         
        :param table: Instance of a CassandraTable
        :param partitions_to_eliminate: List of tuples. Each tuple as a column name as String
        and a list of keys which should be selected. It should only contain columns which are partition columns.
        :param force: It is a boolean. In case all the partitions need to be loaded, which is not recommended, 
        it should be set to 'True'.
        :return: 
        """
        part_cols_prun = dict.fromkeys(table.partition_cols)

        if partitions_to_load is None or len(partitions_to_load) == 0:
            if force is True:
                return
            else:
                raise Exception("ATTENTION: All partitions will be loaded, query might be aborted!!!"
                                + "To proceed re-call the function with force = True.")
        else:
            for partition in partitions_to_load:
                (col, part_keys) = partition
                if col not in table.partition_cols:
                    raise Exception("Column " + str(col) + " is not a partition column. It should be one of "
                                    + str(table.partition_cols) + ".")
                else:
                    try:
                        part_cols_prun[col] = list(map(float, part_keys))
                    except Exception as e:
                        raise("Invalid value in the partition keys list: " + str(e) + " !!!")

        for col in list(part_cols_prun.keys()):
            if col in list(table.partition_cols):
                table.partition_keys = table.partition_keys[table.partition_keys[col].isin(part_cols_prun[col])]
        return

    def build_query(self, table):
        """
        It builds and compiles the query which will be used to load data from a Cassandra table into a Dask Dataframe.
        > build_query(table)
        
        :param table: Instance of CassandraTable.
        :return: 
        """
        if self.projections is None:
            self.sql_query = sql.select([text('*')]).select_from(text(table.name))
        else:
            self.sql_query = sql.select([text(f) for f in self.projections]).select_from(text(table.name))

        if self.and_predicates is not None:
            self.sql_query = self.sql_query.where(sql.expression.and_(*self.and_predicates))
        return

    def print_query(self):
        """
        It prints the query which will be used to load data from a Cassandra table into a Dask Dataframe.
        > print_query()
        
        :return: 
        """
        if self.sql_query is None:
            self.error = "The query needs first to be defined!!! "
            self.finished_event.set()
        else:
            print(self.sql_query.compile(compile_kwargs={"literal_binds": True}))
        return
