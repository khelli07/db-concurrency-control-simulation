from utils import Operation, OpType


class Transaction:
    def __init__(self, name):
        self.t_name = name

    def do_start(self, config):
        print(f"Transaction {self.t_name} starts.")
        config.rw[self.t_name] = {}
        config.rw[self.t_name][OpType.READ] = set()
        config.rw[self.t_name][OpType.WRITE] = set()
        self.ts_start = config.ts.value()
        self.executed_this_far = []
        self.executed_this_far.append(Operation(OpType.READ, self, self.do_start))

    def do_read(self, config, varname):
        tmp = config.ts.value()
        config.ts.increment()

        value = config.resources[varname]
        config.logs[tmp] = (self.t_name, OpType.READ, varname)
        config.rw[self.t_name][OpType.READ].add(varname)

        print(f"{tmp}: Transaction {self.t_name} reads {varname}.")
        self.executed_this_far.append(
            Operation(OpType.READ, self, self.do_read, varname)
        )
        return tmp, value

    def do_write(self, config, varname, value):
        tmp = config.ts.value()
        config.ts.increment()

        config.logs[tmp] = (self.t_name, OpType.WRITE, varname, value)
        config.resources[varname] = value
        config.rw[self.t_name][OpType.WRITE].add(varname)

        print(f"{tmp}: Transaction {self.t_name} writes {varname}.")
        self.executed_this_far.append(
            Operation(OpType.WRITE, self, self.do_write, varname, value)
        )

    def do_validate(self, config):
        tmp = config.ts.value()
        config.ts.increment()

        print(f"{tmp}: Transaction {self.t_name} tries to validate.")

        i = self.ts_start
        config.logs[tmp] = (self.t_name, OpType.VALIDATE)
        read = config.rw[self.t_name][OpType.READ]
        while i < tmp:
            t_name_other, op, *_ = config.logs[i]
            if op == OpType.COMMIT:
                if OpType.WRITE in config.rw[t_name_other]:
                    write = config.rw[t_name_other][OpType.WRITE]
                    if not read.isdisjoint(write):
                        self.executed_this_far.append(
                            Operation(OpType.VALIDATE, self, self.do_validate)
                        )
                        print(
                            f"{tmp}: Transaction {self.t_name} is invalid. Aborting..."
                        )
                        self.do_rollback(config)
                        return False
            i += 1

        print(f"{tmp}: Transaction {self.t_name} validated successfully.")
        return True

    def do_commit(self, config):
        tmp = config.ts.value()
        config.ts.increment()

        print(f"{tmp}: Transaction {self.t_name} commits.")
        config.logs[tmp] = (self.t_name, OpType.COMMIT)

    def do_rollback(self, config):
        print(f"{self.t_name} rolls back.")
        self.execute(config)

    def execute(self, config):
        for op in self.executed_this_far:
            if op.op_type == OpType.VALIDATE:
                is_valid = op.cb(config, *op.args)
                if not is_valid:
                    raise Exception("Validation failed on second try")
            else:
                op.cb(config, *op.args)
