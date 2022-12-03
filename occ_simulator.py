class OCCSimulator:
    def __init__(self, op_queue):
        self.op_queue = op_queue

    def process(self, config):
        for op in self.op_queue:
            op.cb(config, *op.args)
