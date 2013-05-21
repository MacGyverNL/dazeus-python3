import argparse
import dazeus as dz
import logging
import signal
import tulip


@tulip.task
def names(ns):
    dazeus = dz.DaZeus()
    if ns.tcp:
        yield from dazeus.connect("tcp", ns.tcp[0], ns.tcp[1])
    else:
        yield from dazeus.connect("unix", ns.unix)

    yield from dazeus.subscribe_events("NAMES")

    networks = yield from dazeus.networks()
    for network in networks:
        print(network)
        channels = yield from dazeus.channels(network)
        for channel in channels:
            print("    Channel:", channel)
            yield from dazeus.names(network, channel)
            yield from handle_names(dazeus, network, channel)


@tulip.coroutine
def handle_names(dazeus, network, channel):
    while True:
        event = yield from dazeus.read_event()
        if event["event"] != "NAMES":
            print("Not names:", event)
            continue
        params = event["params"]
        in_network = params[0]
        origin = params[1]
        in_channel = params[2]
        if network != in_network or channel != in_channel:
            print("NAMES mismatch, continuing.", network, in_network,
                  channel, in_channel)
        names = params[3:]
        print("        ", len(names), "names on this channel.")
        for name in names:
            print("        ", name)
        return


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

    args = parser.parse_args()

    loop = tulip.get_event_loop()
    loop.run_until_complete(names(args))
