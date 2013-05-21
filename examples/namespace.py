import argparse
import dazeus as dz
import logging
import signal
import tulip


@tulip.task
def namespace(ns):
    dazeus = dz.DaZeus()
    if ns.tcp:
        yield from dazeus.connect("tcp", ns.tcp[0], ns.tcp[1])
    else:
        yield from dazeus.connect("unix", ns.unix)

    keys = yield from dazeus.get_prop_keys(ns.namespace, ns.network,
                                           ns.receiver, ns.sender)
    for key in keys:
        key = ns.namespace + "." + key
        val = yield from dazeus.get_prop(key, ns.network, ns.receiver,
                                         ns.sender)
        print(key, ": ", val)

    print(len(keys), "keys listed.")

if __name__ == "__main__":
    logging.basicConfig(
        format='{asctime} {levelname} @line {lineno}:{message}',
        style='{',
        level=logging.ERROR
    )
    parser = argparse.ArgumentParser(description="DaZeus counter example")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-t", "--tcp", help="Make a TCP connection to the "
                       "provided address and port.", nargs=2)
    group.add_argument("-u", "--unix", help="Make a Unix Domain Socket "
                       "connection to the provided socketfile.")

    parser.add_argument("namespace")
    parser.add_argument("network", nargs="?")
    parser.add_argument("receiver", nargs="?")
    parser.add_argument("sender", nargs="?")
    args = parser.parse_args()

    loop = tulip.get_event_loop()
    loop.run_until_complete(namespace(args))
