import argparse
import dazeus as dz
import signal
import tulip


@tulip.task
def counter(ns):
    dazeus = dz.DaZeus()
    if ns.tcp:
        yield from dazeus.connect("tcp", ns.tcp[0], ns.tcp[1])
    else:
        yield from dazeus.connect("unix", ns.unix)

    _, count = yield from dazeus.get_prop("examples.counter.count")
    if count is None:
        count = 0
    else:
        count = int(count)
    print("Current count is", count)
    count += 1
    yield from dazeus.set_prop("examples.counter.count", count)
    print("Count changed to", count)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DaZeus counter example")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-t", "--tcp", help="Make a TCP connection to the "
                       "provided address and port.", nargs=2)
    group.add_argument("-u", "--unix", help="Make a Unix Domain Socket "
                       "connection to the provided socketfile.")

    args = parser.parse_args()

    loop = tulip.get_event_loop()
    loop.run_until_complete(counter(args))
