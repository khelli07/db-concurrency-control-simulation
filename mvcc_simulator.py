from simulator import TransactionSimulator
from utils import Operation, OpType


class MVCCSimulator(TransactionSimulator):
    def __init__(self, resources):
        super().__init__(resources)
        self.rts = {res: 0 for res in self.resources}
        self.wts = {res: 0 for res in self.resources}
        self.versions = {res: {0} for res in self.resources}
        self.cascading = {}

    def restart(self):
        super().restart()
        self.rts = {res: 0 for res in self.resources}
        self.wts = {res: 0 for res in self.resources}
        self.versions = {res: {0} for res in self.resources}
        self.cascading = {}

    def process(self, op_queue):
        maxval = self.__find_max_ts(op_queue)
        for trans, op in op_queue:
            self.log_operations(trans, op)

            can_execute = True
            if op.op_type == OpType.READ:
                res = op.args[0]
                if self.rts[res] < trans.ts:
                    print(
                        f"    >> Updating R-TS({res}i) from {self.rts[res]} to {trans.ts}"
                    )
                    self.rts[res] = trans.ts
                self.__log_cascading(trans, res)

            elif op.op_type == OpType.WRITE:
                res = op.args[0]
                if trans.ts < self.rts[res]:
                    self.__rollback(res, maxval, trans)
                    can_execute = False
                else:
                    if trans.ts != self.wts[res]:
                        print(
                            f"    >> New version of {res} is created. W-TS({res}i) = {trans.ts}"
                        )
                    self.versions[res].add(trans.ts)
                    self.rts[res] = trans.ts
                    self.wts[res] = trans.ts

            elif op.op_type == OpType.COMMIT:
                self.cascading[trans.ts] = set()

            if can_execute:
                op.cb(*op.args)

    def _log_validate(self, trans, op):
        pass

    def __log_cascading(self, trans, res):
        biggest_before_ts = [ver for ver in self.versions[res] if ver <= trans.ts]
        val = max(biggest_before_ts)
        if val not in self.cascading:
            self.cascading[val] = {trans}

    def __rollback(self, res, maxval, trans):
        print(
            f"    >> Transaction {trans.t_name} tries to write {res} but latest read is {self.rts[res]}. Aborting..."
        )
        print(f"    >> Transaction {trans.t_name} start with new TS = {maxval + 1}")
        self.log_operations(trans, Operation(OpType.ROLLBACK, trans.do_rollback))

        old_ts = trans.ts
        trans.ts = maxval + 1
        maxval += 1

        self.process(trans.executed_this_far)
        if old_ts in self.cascading:
            for trans in self.cascading[old_ts]:
                print(f"===> Cascading rollback to {trans.t_name} <===")
                self.process(trans.executed_this_far)
            print(f"===> Cascading rollback finished. <===")

    def __find_max_ts(self, op_queue):
        ts = [trans.ts for trans, _ in op_queue]
        return max(ts)
