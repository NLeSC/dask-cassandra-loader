class CassandraOperators(object):
    """ Operators for a valida SQL select statement over a Cassandra Table. """

    def __init__(self):
        """ 
        Initialization of CassandraOperators.
         > CassandraOperators()
         
        """
        self.error = None
        self.warning = None
        self.operators = ["less_than_equal", "less_than", "greater_than_equal", "greater_than", "equal", "between",
                          "like", "in_", "notin_"]
        self.si_operators = ["less_than_equal", "less_than", "greater_than_equal", "greater_than", "equal", "like"]
        self.bi_operators = ["between"]
        self.li_operators = ["in_", "notin_"]
        return

    @staticmethod
    def create_predicate(table, col_name, op_name, values):
        """
        It creates a single predicate over a table's column using an operator. Call CassandraOperators.print_operators()
         to print all available operators.
        > create_predicate(table, 'month', 'les_than', 1)
        
        :param table: Instance of CassandraTable.
        :param col_name: Table's column name as string. 
        :param op_name: Operators name as string.
        :param values: List of values. The number of values depends on the operator.
        :return: 
        """
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
        """
        Print all the operators that can be used in a SQL select statement over a Cassandra's table.
        > print_operators()
        
        :return: 
        """
        print("The single value operators - op(x) - are: " + str(self.si_operators) + ".")
        print("The binary operators - op(x,y) - are: " + str(self.bi_operators) + ".")
        print("The list of values operators - op([x,y,...,z]) - are: " + str(self.li_operators) + ".")
