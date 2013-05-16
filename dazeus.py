import collections
import signal
import socket
import os

import logging

import json

import tulip

# TODO: fix decent debugging logging
# TODO: documentation
# TODO: well, most of everything else as well.
# TODO: Errors on reply instead of simply returning from check_reply

PROTOCOL_VERSION = 1


class DaZeus:

    def __init__(self):
        self._subscriptions = []
        self._transport = None  # FIXME remove this if we never use it directly
        self._protocol = None

        self._eventbuffer = tulip.DataBuffer()
        self._replybuffer = tulip.DataBuffer()

        self._connected = False

    @tulip.coroutine
    def connect(self, conntype, address, port=None):
        if conntype not in ("tcp", "unix"):
            #FIXME raise exception
            logging.error("Invalid conntype value: %s. "
                          "Valid values are tcp and unix.", conntype)
            return False

        loop = tulip.get_event_loop()
        #FIXME check for loop == None

        if conntype == "unix":
            usock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            usock.connect(address)
            #FIXME error handling
            transport, protocol = yield from loop.create_connection(
                tulip.StreamProtocol, sock=usock)
        elif conntype == "tcp":
            transport, protocol = yield from loop.create_connection(
                tulip.StreamProtocol, address, port)

        self._messages = protocol.set_parser(dazeus_message_parser())
        self._transport = transport
        self._protocol = protocol

        buffer_router(self._messages, is_event, self._eventbuffer,
                      self._replybuffer)

        self._connected = True
        return True

    @tulip.coroutine
    def read_event(self):
        """Read the first event in the buffer."""
        return (yield from self._eventbuffer.read())

    @tulip.coroutine
    def _read_reply(self):
        """Read the first reply in the buffer."""
        return (yield from self._replybuffer.read())

    @tulip.coroutine
    def networks(self):
        """Returns a list of active networks on this DaZeus instance."""
        req = dazeus_json_create(dazeus_create_request("get", "networks"))
        reply = yield from self._send(req)
        if check_reply("get", "networks", reply):
            return reply["networks"]
        #TODO handle absence of networks.

    @tulip.coroutine
    def channels(self, network):
        """Returns a list of channels joined on the specified network."""
        req = dazeus_json_create(dazeus_create_request("get", "channels",
                                                       network))
        reply = yield from self._send(req)
        if check_reply("get", "channels", reply):
            return reply["channels"]

    @tulip.coroutine
    def message(self, network, recipient, message):
        """Sends the given message to the given recipient on the network.

        Recipient may be a channel (usually prefixed with #) or a nickname.
        """
        req = dazeus_json_create(dazeus_create_request("do", "message",
                                                       network, recipient,
                                                       message))
        reply = yield from self._send(req)
        return check_reply("do", "message", reply)

    @tulip.coroutine
    def action(self, network, recipient, message):
        """Sends message as CTCP ACTION (/me) to the recipient on the network.

        Recipient may be a channel (usually prefixed with #) or a nickname.
        """
        req = dazeus_json_create(dazeus_create_request("do", "action",
                                                       network, recipient,
                                                       message))
        reply = yield from self._send(req)
        return check_reply("do", "action", reply)

    @tulip.coroutine
    def names(self, network, channel):
        """Sends a NAMES command for the given channel and network.

        If the sending succeeds, a NAMES event will be generated somewhere in
        the future using the DaZeus event system.
        """
        req = dazeus_json_create(dazeus_create_request("do", "names",
                                                       network, channel))
        reply = yield from self._send(req)
        return check_reply("do", "names", reply)

    @tulip.coroutine
    def whois(self, network, nickname):
        """Sends a WHOIS command for the given channel and network.

        If the sending succeeds, a WHOIS event will be generated somewhere in
        the future using the DaZeus event system.
        """
        req = dazeus_json_create(dazeus_create_request("do", "whois",
                                                       network, nickname))
        reply = yield from self._send(req)
        return check_reply("do", "whois", reply)

    @tulip.coroutine
    def join(self, network, channel):
        """Sends a JOIN command for the given channel and network.

        If the sending succeeds and the join is successful, a JOIN event will
        be generated somewhere in the future using the DaZeus event system.
        A join is not successful if the channel was already joined.
        """
        req = dazeus_json_create(dazeus_create_request("do", "join",
                                                       network, channel))
        reply = yield from self._send(req)
        return check_reply("do", "join", reply)

    @tulip.coroutine
    def part(self, network, channel):
        """Sends a PART command for the given channel and network.

        If the sending succeeds and the part is successful, a PART event will
        be generated somewhere in the future using the DaZeus event system.
        A part is not successful if the channel was not joined.
        """
        req = dazeus_json_create(dazeus_create_request("do", "part",
                                                       network, channel))
        reply = yield from self._send(req)
        return check_reply("do", "part", reply)

    @tulip.coroutine
    def nick(self, network):
        """Returns the current nickname for DaZeus on the given network."""
        req = dazeus_json_create(dazeus_create_request("get", "nick",
                                                       network))
        reply = yield from self._send(req)
        if check_reply("get", "nick", reply):
            return reply["nick"]

    @tulip.coroutine
    def handshake(self, name, version, configname=None):
        """Performs the (optional) DaZeus handshake.

        The handshake is required for receiving plugin configuration.
        If configname is not given, name is used.
        """
        if configname is None:
            configname = name

        req = dazeus_json_create(dazeus_create_request("do", "handshake",
                                                       name, version,
                                                       PROTOCOL_VERSION,
                                                       configname))
        reply = yield from self._send(req)
        logging.debug("Reply received from handshake: %s", reply)
        return check_reply("do", "handshake", reply)
        #TODO is there an error?

    @tulip.coroutine
    def get_config_var(self, vargroup, varname):
        """Retrieves the given variable from the configuration file.

        vargroup can be "core" or "plugin"; "plugin" can only be used after
        performing a successful handshake using `handshake(...)'
        """
        req = dazeus_json_create(dazeus_create_request("get", "config",
                                                       vargroup, varname))
        reply = yield from self._send(req)
        if check_reply("get", "config", reply):
            return reply["value"]

    @tulip.coroutine
    def get_prop(self, propname, network=None, receiver=None, sender=None):
        """Returns the value of variable from the persistent database.

        An optional context can be passed in the form of network, receiver and
        sender, in that order of specificity. If multiple properties match,
        only the most specific match will be returned.
        """
        req = dazeus_json_create(dazeus_create_request("do", "property",
                                                       "get", network,
                                                       receiver, sender))
        reply = yield from self._send(req)
        if check_reply("do", "property", reply):
            return reply["variable"], reply["value"]
        #TODO values apparently need some form of en / decoding.

    @tulip.coroutine
    def set_prop(self, propname, network=None, receiver=None, sender=None):
        """Sets the given variable in the persistent database.

        An optional context can be passed in the form of network, receiver and
        sender, in that order of specificity, so that multiple properties with
        the same name and partially overlapping contexts can be stored. This is
        useful in situations where you want, e.g. different settings per
        network, or different settings per channel.
        """
        raise NotImplementedError
        #TODO what the hell does the perl code do here?

    @tulip.coroutine
    def del_prop(self, propname, network=None, receiver=None, sender=None):
        """Deletes the given variable from the persistent database.

        An optional context can be passed in the form of network, receiver and
        sender, in that order of specificity. If multiple properties match,
        only the most specific match will be removed. If no properties match,
        no removal will be done.
        """
        # FIXME: Only the most specific, or only an exact match?
        raise NotImplementedError

    @tulip.coroutine
    def get_prop_keys(self, namespace, network=None,
                      receiver=None, sender=None):
        """Retrieves all keys in a given namespace.

        E.g. if example.foo and example.bar were stored earlier,
        get_prop_keys("example") will return ["foo", "bar"].

        An optional context can be passed in the form of network, receiver and
        sender, in that order of specificity. If multiple namespaces match,
        only the most specific match will be returned.
        """
        raise NotImplementedError

    @tulip.coroutine
    def subscribe_events(self, *events):
        """Subscribe to the given events."""
        req = dazeus_json_create(dazeus_create_request("do", "subscribe",
                                                       *events))
        reply = yield from self._send(req)
        if check_reply("do", "subscribe", reply):
            numsub = reply["added"]
            self._subscriptions.extend(events)
            #TODO check if there can be duplicates.
            return events

    @tulip.coroutine
    def unsubscribe_events(self, *events):
        """Unsubscribe from the given events. They will no longer be received,
        unless subscribed to again. Any unprocessed events still waiting to be
        read from the buffer will not be removed.
        """
        req = dazeus_json_create(dazeus_create_request("do", "unsubscribe",
                                                       *events))
        reply = yield from self._send(req)
        if check_reply("do", "unsubscribe", reply):
            numsub = reply["removed"]
            for event in events:
                self._subscriptions.remove(event)
            #TODO check if there can be duplicates.
            return events

    @tulip.coroutine
    def subscribe_command(self, command, network=None):
        """Subscribe to a specific command event.

        This is useful for plugins that do not want to handle all PRIVMSGs
        sent in channels, but only their own commands.
        """
        #TODO: There's more arguments than just command and network. Find
        # out what they mean.
        params = ["do", "command", command]
        if network is not None:
            params.add(network)
        req = dazeus_json_create(dazeus_create_request(*params))
        reply = yield from self._send(req)
        return check_reply("do", "command", reply)

    def subscriptions(self):
        """Returns a list of all events which you are subscribed to."""
        return self._subscriptions

    @tulip.coroutine
    def _send(self, req):
        self._transport.write(req)
        return (yield from self._read_reply())


def check_reply(reqtype, what, reply):
    logging.debug("Reqtype passed to check_reply: %s", reqtype)
    logging.debug("Reply passed to check_reply: %s", reply)

    if reply is None:
        #TODO make this an exception
        logging.error("EOF received while waiting for reply.")
        return False

    if reqtype == "get":
        reptype = "got"
    elif reqtype == "do":
        reptype = "did"
    else:
        #TODO handle invalid reqtype
        logging.warning("Invalid reqtype: %s", reqtype)
    got = reply.get(reptype)
    if got != what:
        logging.error("Invalid reply received: %s", reply)
        #TODO make this an exception
        return False
    success = reply.get("success")
    if not success:
        logging.warning("Getting %s failed: %s", what, reply)
        #TODO make this an exception
        # use reply.get("error").
        return False
    return True


def dazeus_json_create(message_dict):
    logging.debug("Creating json from dict: %s", message_dict)
    msg = json.dumps(message_dict)
    logging.debug("Json to send: %s", msg)
    msg = msg.encode('utf-8')
    logging.debug("Encoded json: %s", msg)
    logging.debug("Length of json: %s", len(msg))

    length = str(len(msg))
    length = length.encode('utf-8')
    msg = length + msg
    logging.debug("Complete message to send: %s", msg)

    return msg


def dazeus_create_request(reqtype, what, *params):
    if reqtype not in ("do", "get"):
        #TODO fail on invalid reqtype
        logging.warning("Invalid reqtype: %s", reqtype)
    msg = {reqtype: what}
    if params is not None:
        msg["params"] = params
    return msg


def parse_json(json_raw):
    logging.debug("Parsing json: %s", json_raw)

    json_dict = json.loads(json_raw)
    logging.debug("Resulting dictionary: %s", json_dict)

    return json_dict


def dazeus_message_parser():
    """Read DaZeus messages from the stream.

    Read DaZeus messages from the stream, parse their JSON.
    Fixme: which exceptions can be raised?
    Fixme: return value?

    See tulip/parsers.py and tulip/http/protocol.py for rationale.
    """

    out, buf = yield

    try:
        # FIXME correctly ignore \n and \r outside of JSON
        # FIXME handle EOF (empty buffer)
        # FIXME handle incorrect JSON size
        while True:
            # read size of the JSON message, at most 20 bytes of ASCII-digits
            # buf.readuntil also yields the delimiting character, strip that.
            json_size = yield from buf.readuntil(b'{', 20, BufferError)
            logging.debug("Received sizefield: %s", json_size)
            json_size = json_size.decode('ascii')[:-1]
            logging.debug("Decoded sizefield from bytes: %s", json_size)

            if not json_size.isdecimal():
                json_size = json_size.lstrip("\r\n")

            #TODO better handling for invalid characters in stream.
            json_size = int(json_size)
            logging.debug("Integer value of size: %s", json_size)

            # read the JSON message itself, terminating at the delimiting
            # character or at json_size.
            #FIXME throw an exception if the delimiter is not found at
            # json_size.
            json_raw = b'{' + (yield from buf.read(json_size - 1))
            json_raw = json_raw.decode('utf-8')
            if json_raw[-1] != '}':
                logging.error("} not found at expected location: %s", json_raw)

            # Parse the raw JSON
            # TODO: invalid json handling
            logging.debug("Parsing json: %s", json_raw)
            json_dict = json.loads(json_raw)
            logging.debug("Resulting dictionary: %s", json_dict)

            # Push the JSON dictionary onto the application's databuffer.
            out.feed_data(json_dict)
    except tulip.EofStream as e:
        logging.exception("EofStream exception caught while parsing "
                          "protocol messages: %s", e)
        out.feed_eof()
    except Exception as e:
        logging.exception("Other exception caught while parsing protocol "
                          "messages: %s", e)


def is_event(message):
    return "event" in message


@tulip.task
def buffer_router(inbuffer, selector, truebuffer, falsebuffer):
    """Read messages from the inbuffer and feed them to an output buffer.

    inbuffer, truebuffer and falsebuffer are tulip.DataBuffer.
    selector is a method that takes one argument, the message, and returns a
    boolean. If selector(message) returns True, the message is fed to
    truebuffer, falsebuffer otherwise.
    """
    try:
        while True:
            message = yield from inbuffer.read()
            if message is None:
                logging.debug("Routed stream closed. Feeding EOF to buffers.")
                truebuffer.feed_eof()
                falsebuffer.feed_eof()
                break

            if selector(message):
                logging.debug("Fed message %s to truebuffer", message)
                truebuffer.feed_data(message)
            else:
                logging.debug("Fed message %s to falsebuffer", message)
                falsebuffer.feed_data(message)
    except Exception as e:
        logging.exception("Exception caught while parsing "
                          "protocol messages: %s", e)
        truebuffer.feed_eof()
        falsebuffer.feed_eof()


@tulip.task
def networkzeus():
    dazeus = DaZeus()
#   connection = yield from dazeus.connect("unix", "/tmp/pythonzeustest.sock")
    connection = yield from dazeus.connect("tcp", "localhost", 1234)
    logging.debug("Connection established %s", connection)

    handshake = yield from dazeus.handshake("networkzeus", "0.0.1")

    # register for }test
    events = yield from dazeus.subscribe_command("test")

    events = yield from dazeus.subscribe_events("whois", "names",
                                                "join", "part", "privmsg")
    print("Subscribed to events: %s", events)

    networks = yield from dazeus.networks()
    print(networks)
    for network in networks:
        parted = yield from dazeus.part(network, "#dazeus")
        joined = yield from dazeus.join(network, "#dazeus")
        nick = yield from dazeus.nick(network)
        channels = yield from dazeus.channels(network)
        for channel in channels:
#            message = yield from dazeus.message(network, channel, "Test")
#            action = yield from dazeus.action(network, channel,
#                                              "tests some more")
            names = yield from dazeus.names(network, channel)


if __name__ == "__main__":
    logging.basicConfig(
        format='{asctime} {levelname} @line {lineno}:{message}',
        style='{',
        level=logging.DEBUG
    )
    # Setup the event loop
    loop = tulip.get_event_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, loop.stop)
    except RuntimeError:
        logging.critical("RuntimeError caught when trying to add "
                         "signal handler to event loop")

    networkzeus()

    loop.run_forever()
