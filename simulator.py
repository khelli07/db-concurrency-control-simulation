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

    def log_operations(self, trans, op):
        if op.op_type == OpType.START:
            self.rw[trans.t_name] = {}
            self.rw[trans.t_name][OpType.READ] = set()
            self.rw[trans.t_name][OpType.WRITE] = set()
        else:
            if op.op_type == OpType.READ:
                self.__log_read(trans, op)
            elif op.op_type == OpType.WRITE:
                self.__log_write(trans, op)
            elif op.op_type == OpType.VALIDATE:
                self.__log_validate(trans, op)
            elif op.op_type == OpType.COMMIT:
                self.__log_commit(trans, op)
            elif op.op_type == OpType.ROLLBACK:
                self.__log_rollback(trans, op)

    def __log_read(self, trans, op):
        tmp = self.__get_and_increment_ts()
        varname = op.args[0]
        self.logs[tmp] = (trans.t_name, OpType.READ, varname)
        self.rw[trans.t_name][OpType.READ].add(varname)
        print(f"{tmp}: Transaction {trans.t_name} reads {varname}.")

    def __log_write(self, trans, op):
        tmp = self.__get_and_increment_ts()
        varname, value = op.args
        self.resources[varname] = value
        self.logs[tmp] = (trans.t_name, OpType.WRITE, varname, value)
        self.rw[trans.t_name][OpType.WRITE].add(varname)
        print(f"{tmp}: Transaction {trans.t_name} writes {varname}.")

    def __log_validate(self, trans, op):
        tmp = self.__get_and_increment_ts()
        print(f"{tmp}: Transaction {trans.t_name} tries to validate.")
        self.logs[tmp] = (trans.t_name, OpType.VALIDATE)

    def __log_commit(self, trans, op):
        tmp = self.__get_and_increment_ts()
        print(f"{tmp}: Transaction {trans.t_name} commits.")
        self.logs[tmp] = (trans.t_name, OpType.COMMIT)

    def __log_rollback(self, trans, op):
        tmp = self.__get_and_increment_ts()
        print(f"{tmp}: Transaction {trans.t_name} rolls back. 🔙")
        self.logs[tmp] = (trans.t_name, OpType.ROLLBACK)

    def __get_and_increment_ts(self):
        tmp = self.ts.value()
        self.ts.increment()
        return tmp
