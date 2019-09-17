class CassandraOperators(object):

    def __init__(self):
        self.error = None
        self.warning = None
        self.operators = ["less_than_equal", "less_than", "greater_than_equal", "greater_than", "equal", "between",
                          "like", "in_", "notin_"]
        self.si_operators = ["less_than_equal", "less_than", "greater_than_equal", "greater_than", "equal", "like"]
        self.bi_operators = ["between"]
        self.li_operators = ["in_", "notin_"]
        return

    # Predicates only for columns which are not primary key columns:
    @staticmethod
    def create_predicate(self, table, op_name, col_name, values):
        if op_name == "less_than_equal":
            return table.predicate_cols[col_name] <= values[0]
        elif op_name == "less_than":
            return table.predicate_cols[col_name] < values[0]
        elif op_name == "greater_than_equal":
            return table.predicate_cols[col_name] >= values[0]
        elif op_name == "greater_than":
            return table.predicate_cols[col_name] > values[0]
        elif op_name == "equal":
            return table.predicate_cols[col_name] == values[0]
        elif op_name == "between":
            return table.predicate_cols[col_name].between(values[0], values[1])
        elif op_name == "like":
            return table.predicate_cols[col_name].like(values[0])
        elif op_name == "in_":
            return table.predicate_cols[col_name].in_(values)
        elif op_name == "notin_":
            return table.predicate_cols[col_name].notin_(values)
        else:
            raise Exception("Invalid operator!!!")
        return

    def print_operators(self):
        print("The single value operators - op(x) - are: " + str(self.si_operators) + ".")
        print("The binary operators - op(x,y) - are: " + str(self.bi_operators) + ".")
        print("The list of values operators - op([x,y,...,z]) - are: " + str(self.li_operators) + ".")
