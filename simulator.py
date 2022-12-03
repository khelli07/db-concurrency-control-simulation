from timestamp import Timestamp
from utils import OpType


class TransactionSimulator:
    def __init__(self, resources):
        self.resources = resources
        self.ts = Timestamp()
        self.logs = {}
        self.rw = {}

    def restart(self):
        self.ts = Timestamp()
        self.logs = {}
        self.rw = {}

    def process(self, op_queue):
        raise NotImplementedError

    def write_history(self, trans, op):
        if op.op_type == OpType.START:
            self.rw[trans.t_name] = {}
            self.rw[trans.t_name][OpType.READ] = set()
            self.rw[trans.t_name][OpType.WRITE] = set()
        else:
            tmp = self.ts.value()
            self.ts.increment()
            if op.op_type == OpType.READ:
                varname = op.args[0]
                self.logs[tmp] = (trans.t_name, OpType.READ, varname)
                self.rw[trans.t_name][OpType.READ].add(varname)
                print(f"{tmp}: Transaction {trans.t_name} reads {varname}.")
            elif op.op_type == OpType.WRITE:
                varname, value = op.args
                self.logs[tmp] = (trans.t_name, OpType.WRITE, varname, value)
                self.resources[varname] = value
                self.rw[trans.t_name][OpType.WRITE].add(varname)
                print(f"{tmp}: Transaction {trans.t_name} writes {varname}.")
            elif op.op_type == OpType.VALIDATE:
                print(f"{tmp}: Transaction {trans.t_name} tries to validate.")
                self.logs[tmp] = (trans.t_name, OpType.VALIDATE)
            elif op.op_type == OpType.COMMIT:
                print(f"{tmp}: Transaction {trans.t_name} commits.")
                self.logs[tmp] = (trans.t_name, OpType.COMMIT)
            elif op.op_type == OpType.ROLLBACK:
                print(f"{tmp}: Transaction {trans.t_name} rolls back. 🔙")
                self.logs[tmp] = (trans.t_name, OpType.ROLLBACK)
