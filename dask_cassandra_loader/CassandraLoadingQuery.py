from . import CassandraOperators
from sqlalchemy import sql
from sqlalchemy.sql import text


class CassandraLoadingQuery(object):

    def __init__(self):
        self.error = None
        self.warning = None
        self.projections = None
        self.and_predicates = None
        self.sql_query = None
        return

    def set_projections(self, table, projections):
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
        self.projections = None
        return

    def set_and_predicates(self, table, predicates):
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
                    self.predicates.append(operators.create_predicate(table, op, col, values))
        return

    def remove_and_predicates(self):
        self.and_predicates = None
        return

    @staticmethod
    def partition_elimination(self, table, partitions_to_eliminate, force):
        part_cols_prun = dict.fromkeys(table.partition_cols)

        if partitions_to_eliminate is None or len(partitions_to_eliminate) == 0:
            if force is True:
                return
            else:
                raise Exception("ATTENTION: All partitions will be loaded, query might be aborted!!!"
                                + "To proceed re-call the function with force = True.")
        else:
            for partition in partitions_to_eliminate:
                (col, part_keys) = partition
                if col not in table.partition_cols:
                    raise Exception("Column " + str(col) + " is not a partition column. It should be one of "
                                    + str(table.partition_cols) + ".")
                else:
                    try:
                        part_cols_prun[col] = list(map(float, part_keys))
                    except Exception as e:
                        raise("Invalid value in the partition keys list: " + str(e) + " !!!")

        # It prunes the partition keyvalues using a Dictionary passed as argument.
        # part_cols_prun is a dictionary (partition_col_name, values_to_prune).
        for col in list(part_cols_prun.keys()):
            if col in list(table.partition_cols):
                table.partition_keys = table.partition_keys[table.partition_keys[col].isin(part_cols_prun[col])]
        return

    # Build the query
    def build_query(self, table):
        and_predicate_query = ""
        if self.projections is None:
            self.sql_query = sql.select([text('*')]).select_from(text(table.name))
        else:
            self.sql_query = sql.select([text(f) for f in self.projections]).select_from(text(table.name))

        if self.and_predicates is not None:
            self.sql_query = self.sql_query.where(sql.expression.and_(*self.and_predicates))
        return

    def print_query(self):
        if self.sql_query is None:
            self.error = "The query needs first to be defined!!! "
            self.finished_event.set()
        else:
            print(self.sql_query.compile(compile_kwargs={"literal_binds": True}))
        return
