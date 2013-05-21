import argparse
import dazeus as dz
import signal
import tulip


@tulip.task
def monitor(ns):
    dazeus = dz.DaZeus()
    if ns.tcp:
        yield from dazeus.connect("tcp", ns.tcp[0], ns.tcp[1])
    else:
        yield from dazeus.connect("unix", ns.unix)

    yield from dazeus.subscribe_events("CONNECT", "DISCONNECT", "JOIN", "PART",
                                       "QUIT", "NICK", "MODE", "TOPIC",
                                       "INVITE", "KICK", "PRIVMSG", "NOTICE",
                                       "CTCP", "CTCP_REP", "ACTION", "NUMERIC",
                                       "UNKNOWN", "WHOIS", "NAMES",
                                       "PRIVMSG_ME", "CTCP_ME", "ACTION_ME")

    while True:
        event = yield from dazeus.read_event()
        if not event:
            return
        print(event)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DaZeus counter example")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-t", "--tcp", help="Make a TCP connection to the "
                       "provided address and port.", nargs=2)
    group.add_argument("-u", "--unix", help="Make a Unix Domain Socket "
                       "connection to the provided socketfile.")

    args = parser.parse_args()

    loop = tulip.get_event_loop()
    loop.run_until_complete(monitor(args))
