from utils import Operation, OpType


class Transaction:
    def __init__(self, name):
        self.t_name = name

    def do_start(self, begin_time):
        print(f"Transaction {self.t_name} starts.")
        self.ts_start = begin_time
        self.executed_this_far = []
        self.executed_this_far.append((self, Operation(OpType.START, self.do_start)))

    def do_read(self, varname):
        self.executed_this_far.append(
            (self, Operation(OpType.READ, self.do_read, varname))
        )

    def do_write(self, varname, value):
        self.executed_this_far.append(
            (self, Operation(OpType.WRITE, self.do_write, varname, value))
        )

    def do_validate(self):
        self.executed_this_far.append(
            (self, Operation(OpType.VALIDATE, self.do_validate))
        )

    def do_commit(self):
        pass

    def do_rollback(self):
        pass
