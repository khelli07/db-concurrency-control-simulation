from enum import Enum


class OpType(Enum):
    START = 0
    READ = 1
    WRITE = 2
    VALIDATE = 3
    COMMIT = 4
    ABORT = 5


class Operation:
    def __init__(self, op_type: OpType, trans, cb, *args):
        self.op_type = op_type
        self.trans = trans
        self.cb = cb
        self.args = args
