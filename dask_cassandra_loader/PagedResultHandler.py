from threading import Event

class PagedResultHandler(object):
    def __init__(self, future):
        self.error = None
        self.finished_event = Event()
        self.future = future
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error)
        self.df = None

    def handle_page(self, rows):
        if self.df is None:
            self.df = rows
        else:
            self.df = self.df.append(rows, ignore_index=True)

        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

    def handle_error(self, exc):
        self.error = exc
        self.finished_event.set()
