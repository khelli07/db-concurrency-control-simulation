from timestamp import Timestamp


class Config:
    def __init__(self, resources):
        self.resources = resources
        self.ts = Timestamp()
        self.logs = {}
        self.rw = {}
