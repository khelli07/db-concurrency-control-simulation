from config import *
from occ_simulator import OCCSimulator
from reader import Reader

if __name__ == "__main__":
    reader = Reader("test/tc1.txt")
    reader.read()
    queue, resources, trans_exec = reader.result()

    os = OCCSimulator(queue, trans_exec)
    conf = Config(resources)
    os.process(conf)
