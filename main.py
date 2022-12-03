from mvcc_simulator import MVCCSimulator
from occ_simulator import OCCSimulator
from reader import Reader

if __name__ == "__main__":
    reader = Reader("test/tc1.txt")
    reader.read()
    queue, resources = reader.result()

    print("=" * 50)
    print("OCC Simulator starts...")
    print("=" * 50)
    os = OCCSimulator(resources)
    os.process(queue)

    print("=" * 50)
    print("MVCC Simulator starts...")
    print("=" * 50)
    mvcc = MVCCSimulator(resources)
    mvcc.process(queue)
