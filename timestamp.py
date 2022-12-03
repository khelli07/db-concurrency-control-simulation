from threading import Lock


class Timestamp:
    # constructor
    def __init__(self):
        # initialize counter
        self._counter = 1
        # initialize lock
        self._lock = Lock()

    # increment the counter
    def increment(self):
        with self._lock:
            self._counter += 1

    # get the counter value
    def value(self):
        with self._lock:
            return self._counter
