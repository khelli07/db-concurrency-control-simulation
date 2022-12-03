from simulator import TransactionSimulator
from utils import Operation, OpType


class MVCCSimulator(TransactionSimulator):
    def __init__(self, resources):
        super().__init__(resources)
        self.rts = {res: 0 for res in self.resources}
        self.wts = {res: 0 for res in self.resources}

    def restart(self):
        super().restart()
        self.rts = {res: 0 for res in self.resources}
        self.wts = {res: 0 for res in self.resources}

    def process(self, op_queue):
        maxval = self.find_max_ts(op_queue)
        for trans, op in op_queue:
            can_execute = True

            self.log_operations(trans, op)
            if op.op_type == OpType.READ:
                res = op.args[0]
                if self.rts[res] < trans.ts:
                    print(f"Updating R-TS({res}i) from {self.rts[res]} to {trans.ts}")
                    self.rts[res] = trans.ts
            elif op.op_type == OpType.WRITE:
                res = op.args[0]
                if trans.ts < self.rts[res]:
                    self.log_operations(
                        trans, Operation(OpType.ROLLBACK, trans.do_rollback)
                    )
                    print(
                        f"Transaction {trans.t_name} tries to write {res} but latest read is {self.rts[res]}. Aborting..."
                    )
                    print(
                        f"Transaction {trans.t_name} start with new TS = {maxval + 1}"
                    )
                    trans.ts = maxval + 1
                    maxval += 1
                    self.process(trans.executed_this_far)
                    can_execute = False
                else:
                    self.rts[res] = trans.ts
                    self.wts[res] = trans.ts
                    if trans.ts != self.wts[res]:
                        print(
                            f"New version of {res} is created. W-TS({res}i) = {self.wts[res]}"
                        )
            if can_execute:
                op.cb(*op.args)

    def find_max_ts(self, op_queue):
        ts = [trans.ts for trans, _ in op_queue]
        return max(ts)

    def __log_validate(self, trans, op):
        pass
