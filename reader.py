import re
from typing import List

from transaction import Transaction
from utils import Operation, OpType

DEFAULT_VALUE = 1


class Reader:
    def __init__(self, filename: str):
        self.filename = filename

    def read(self):
        f = open("test/tc1.txt", "r")

        _ = int(f.readline())
        resources = {r: 0 for r in f.readline().strip().split()}

        operations = []
        for line in f:
            if not line.strip():
                continue
            sp = re.split(r"[()]", line.strip())
            op = sp[0]
            if op[0] != "C":
                res = sp[1]
                operations.append((op[0], op[1:], res))
            else:
                operations.append((op[0], op[1:]))

        f.close()
        self.generate(resources, operations)

    def generate(
        self,
        resources: dict,
        operations: List[tuple],
    ):
        self.trans = {}
        self.resources = resources
        self.op_queue = []
        for op in operations:
            tname = f"T{op[1]}"

            if tname not in self.trans:
                self.trans[tname] = Transaction(tname, int(op[1]))
                ops = Operation(OpType.START, self.trans[tname].do_start)
                self.op_queue.append((self.trans[tname], ops))

            if op[0] == "R":
                ops = Operation(
                    OpType.READ,
                    self.trans[tname].do_read,
                    str(op[2]),
                )
            elif op[0] == "W":
                ops = Operation(
                    OpType.WRITE,
                    self.trans[tname].do_write,
                    str(op[2]),
                    DEFAULT_VALUE,
                )
            elif op[0] == "C":
                ops = Operation(
                    OpType.VALIDATE,
                    self.trans[tname].do_validate,
                )
                self.op_queue.append((self.trans[tname], ops))

                ops = Operation(OpType.COMMIT, self.trans[tname].do_commit)

            self.op_queue.append((self.trans[tname], ops))

    def result(self):
        return self.op_queue, self.resources
