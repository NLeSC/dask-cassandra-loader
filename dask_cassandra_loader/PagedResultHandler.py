from threading import Event


class PagedResultHandler(object):
    """ An handler for paged loading of a Cassandra's query result. """

    def __init__(self, future):
        """
        Initialization of PagedResultHandler
        > handler = PagedResultHandler(future)

        :param future: Future from Cassandra session asynchronous execution. 
        """
        self.error = None
        self.finished_event = Event()
        self.future = future
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error)
        self.df = None

    def handle_page(self, rows):
        """
        It pages the result of a Cassandra query.
        > handle_page(rows)
        
        :param rows: Cassandra's query result. 
        :return: 
        """
        if self.df is None:
            self.df = rows
        else:
            self.df = self.df.append(rows, ignore_index=True)

        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

    def handle_error(self, exc):
        """
        It handles and exception.
        > handle_error(exc)
        
        :param exc: It is a Python Exception. 
        :return: 
        """
        self.error = exc
        self.finished_event.set()
