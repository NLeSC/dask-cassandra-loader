from . import CassandraOperators
from sqlalchemy import sql
from sqlalchemy.sql import text

# For a single table
class CassandraLoadingQuery(object):
    def __init__(self):
        self.error = None
        self.warning = None
        self.projections = None
        self.and_predicates = None
        self.sql_query = None

    def set_projections(self, table):
        done = False
        while (done == False):
            option = input("Do you want to set the list of columns to project? [yes | no]")
            if option not in ["yes", "no"]:
                print("Invalid Option!!!")
            elif (option == "yes"):
                try:
                    my_list = []
                    col = input("Please use one of the following columns: " + str(
                        table.cols) + "!!!\n" + "Projections columns (Enter to exit):\n")
                    if (col == ""):
                        print("All columns will be projected!!!")
                        my_list = str(table.cols)
                        done = True
                    elif col not in table.cols:
                        raise
                    else:
                        my_list.append(str(col))
                        while True:
                            col = input()
                            if (col == ""):
                                break
                            elif col not in table.cols:
                                my_list = []
                                raise
                            elif col in my_list:
                                print("Do not add the same column twice!!!")
                            else:
                                my_list.append(str(col))
                except:
                    print("Invalid column name: '" + col + "'!!!")
                    print("Please use one of the following columns: " + str(table.cols) + "!!!")
                else:
                    self.projections = my_list
                    done = True
            else:
                print("All columns will be projected!!!")
                self.projections = str(table.cols)
                done = True

    def drop_projections(self):
        self.projections = None

    # Functions to manage and_predicates and prune partitions
    def set_and_predicates(self, table):
        done = False
        self.predicates = []
        while (done == False):
            option = input("Do you want to add predicates over the non partition key columns? [yes | no]")
            if option not in ["yes", "no"]:
                print("Invalid Option!!!")
            elif (option == "yes"):
                finished = False
                while (finished == False):
                    operators = CassandraOperators()
                    op = input("Pick an operator " + str(operators.operators).replace("'", "") + " \n(Enter to exit):")
                    if op == "":
                        finished = True
                    elif (op not in operators.operators):
                        print("Incorrect operator!!!")
                        break
                    else:
                        col = input("Pick a non-primary key column " + str(table.predicate_cols.keys()).replace("'",
                                                                                                                "") + " \n(Enter to exit):")
                        if col == "":
                            finished = True
                        elif (col not in table.predicate_cols):
                            print("Incorrect column!!!")
                            break
                        else:
                            try:
                                if (op in operators.si_operators):
                                    values = [float(input("Enter value: "))]
                                    finished = True
                                elif (op in operators.bi_operators):
                                    values = [float(input("Enter bottom limit: ")), float(input("Enter upper limit: "))]
                                    finished = True
                                elif (op in operators.li_operators):
                                    values = list(map(float, input(
                                        "Enter the list of values seperated by space: ").strip().split()))
                                    finished = True
                            except:
                                print("Only numbers are allowed!!!")
                                break
                            else:
                                self.predicates.append(operators.create_predicate(table, op, col, values))
                                done = finished
            else:
                print("No predicates defined!!!")
                done = True

    def remove_and_predicates(self):
        self.and_predicates = None

    # Functions to manage partitions
    def partition_elimination(self, table):
        done = False
        part_cols_prun = dict.fromkeys(table.partition_cols)
        while (done == False):
            option = input("Do you want to do partition elimination? [yes | no]")
            if option not in ["yes", "no"]:
                print("Invalid Option!!!")
            elif (option == "yes"):
                col = input(
                    "Pick a primary key column " + str(table.partition_cols).replace("'", "") + " \n(Enter to exit):")
                if col == "":
                    proceed = input(
                        "ATTENTION: All partitions will be loaded, query might be aborted! Do you want to proceed? [yes | no]")
                    if proceed not in ["yes", "no"]:
                        print("Invalid Option, option 'no' will be used!!!")
                    elif proceed == "yes":
                        done = True
                elif (col not in table.partition_cols):
                    print("Incorrect column!!!")
                    continue
                else:
                    try:
                        part_keys = list(
                            map(float, input("Enter the partition keys seperated by space: ").strip().split()))
                    except:
                        print("Only numbers are allowed!!!")
                    else:
                        part_cols_prun[col] = part_keys
                        finished = False
                        while (finished == False):
                            col = input("Pick a primary key column " + str(table.partition_cols).replace("'",
                                                                                                         "") + " \n(Enter to exit):")
                            if col == "":
                                finished = True
                            elif (col not in table.partition_cols):
                                print("Incorrect column!!!")
                                break
                            else:
                                try:
                                    part_keys = list(map(float, input(
                                        "Enter the partition keys seperated by space: ").strip().split()))
                                except:
                                    print("Only numbers are allowed!!!")
                                    break
                                else:
                                    part_cols_prun[col] = part_keys
                                    done = finished
                        done = finished
            else:
                proceed = input(
                    "ATTENTION: All partitions will be loaded, query might be aborted! Do you want to proceed? [yes | no]")
                if proceed not in ["yes", "no"]:
                    print("Invalid Option, option 'no' will be used!!!")
                elif proceed == "yes":
                    done = True

        # It prunes the partition keyvalues using a Dictionary passed as argument.
        # part_cols_prun is a dictionary (partition_col_name, values_to_prune).
        for col in list(part_cols_prun.keys()):
            if col in list(table.partition_cols):
                table.partition_keys = table.partition_keys[table.partition_keys[col].isin(part_cols_prun[col])]

    # Build the query
    def build_query(self, table):
        and_predicate_query = ""
        if self.projections is None:
            self.sql_query = sql.select([text('*')]).select_from(text(table.name))
        else:
            self.sql_query = sql.select([text(f) for f in self.projections]).select_from(text(table.name))

        if self.and_predicates is not None:
            self.sql_query = self.sql_query.where(sql.expression.and_(*self.and_predicates))

    def print_query(self):
        if self.sql_query is None:
            self.error = "The query needs first to be defined!!! "
            self.finished_event.set()
        else:
            print(self.sql_query.compile(compile_kwargs={"literal_binds": True}))
