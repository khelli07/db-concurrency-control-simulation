from enum import Enum


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
