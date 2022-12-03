from simulator import TransactionSimulator
from utils import Operation, OpType


class OCCSimulator(TransactionSimulator):
    def process(self, op_queue):
        for trans, op in op_queue:
            self.write_history(trans, op)
            if op.op_type == OpType.START:
                op.cb(self.ts.value())
            else:
                op.cb(*op.args)

            if op.op_type == OpType.VALIDATE:
                is_valid = self.validate(trans)
                if not is_valid:
                    print(f"    >> Transaction {trans.t_name} is invalid. Aborting...")
                    self.write_history(
                        trans, Operation(OpType.ROLLBACK, trans.do_rollback)
                    )
                    self.process(trans.executed_this_far)

    def validate(self, trans):
        i = trans.ts_start
        current_ts = self.ts.value()
        read = self.rw[trans.t_name][OpType.READ]
        while i < current_ts:
            t_name_other, op, *_ = self.logs[i]
            if op == OpType.COMMIT:
                if OpType.WRITE in self.rw[t_name_other]:
                    write = self.rw[t_name_other][OpType.WRITE]
                    if not read.isdisjoint(write):
                        joint = [var for var in read if var in write]
                        print(f"    >> {joint} are written by {t_name_other}.")
                        return False
            i += 1

        return True
