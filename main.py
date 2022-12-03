from occ_simulator import OCCSimulator
from reader import Reader

if __name__ == "__main__":
    reader = Reader("test/tc1.txt")
    reader.read()
    queue, resources = reader.result()

    os = OCCSimulator(resources)
    os.process(queue)
