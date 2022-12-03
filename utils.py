from enum import Enum
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


class OpType(Enum):
    START = 0
    READ = 1
    WRITE = 2
    VALIDATE = 3
    COMMIT = 4
    ROLLBACK = 5


class Operation:
    def __init__(self, op_type: OpType, cb, *args):
        self.op_type = op_type
        self.cb = cb
        self.args = args
