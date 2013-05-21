import argparse
import dazeus as dz
import logging
import signal
import tulip


@tulip.task
def scope(ns):
    dazeus = dz.DaZeus()
    if ns.tcp:
        yield from dazeus.connect("tcp", ns.tcp[0], ns.tcp[1])
    else:
        yield from dazeus.connect("unix", ns.unix)

    yield from dazeus.set_prop("examples.scope.spam", "eggs")
    print("Set global scope to eggs")
    yield from dazeus.set_prop("examples.scope.spam", "ham", "kassala")
    print("Set network scope for `oftc' to ham")

    _, val = yield from dazeus.get_prop("examples.scope.spam", "q", "#moo")
    assert val == "eggs"
    _, val = yield from dazeus.get_prop("examples.scope.spam", "kassala",
                                        "#moo")
    assert val == "ham"

    yield from dazeus.del_prop("examples.scope.spam", "oftc")
    print("Unset network scope for `oftc'")
    _, val = yield from dazeus.get_prop("examples.scope.spam", "kassala",
                                        "#moo")
    assert val == "eggs"

    yield from dazeus.del_prop("examples.scope.spam")
    print("Unset global scope")
    yield from dazeus.set_prop("examples.scope.spam", "bacon", "kassala")
    print("Set network scope for `oftc' to bacon")
    _, val = yield from dazeus.get_prop("examples.scope.spam", "q")
    assert val is None

    #clean up
    dazeus.del_prop("examples.scope.spam", "oftc")


if __name__ == "__main__":
    logging.basicConfig(
        format='{asctime} {levelname} @line {lineno}:{message}',
        style='{',
        level=logging.DEBUG
    )
    parser = argparse.ArgumentParser(description="DaZeus counter example")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-t", "--tcp", help="Make a TCP connection to the "
                       "provided address and port.", nargs=2)
    group.add_argument("-u", "--unix", help="Make a Unix Domain Socket "
                       "connection to the provided socketfile.")

    args = parser.parse_args()

    loop = tulip.get_event_loop()
    loop.run_until_complete(scope(args))
