import collections
import signal
import socket
import os

import logging

import json

import tulip

# TODO: fix decent debugging logging
# TODO: documentation

PROTOCOL_VERSION = 1


class DaZeus:

    def __init__(self):
        self._subscriptions = set()
        self._transport = None
        self._protocol = None

        self._eventbuffer = None
        self._replybuffer = None

        self._connected = False

    @tulip.coroutine
    def connect(self, conntype, address, port=None):
        assert conntype in ("tcp", "unix"), ("Invalid connection type passed "
                                             "to connect.")

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

        self._eventbuffer = tulip.DataBuffer()
        self._replybuffer = tulip.DataBuffer()

        buffer_router(self._messages, is_event, self._eventbuffer,
                      self._replybuffer)

        self._connected = True
        return True

    @tulip.coroutine
    def disconnect(self, discard=False):
        """Disconnect the socket."""
        if self._connected:
            transport = self._transport
            # Writing an EOF will trigger a close() at dazeus-core.
            # Best-effort: Triggering a close() in this way ensures that
            # dazeus-core has read all the buffered requests and at least
            # buffered replies to be sent before reading the EOF.
            yield from transport.close()
            self._connected = False
            self._transport = None
            self._protocol = None
            self._subscriptions = set()
        if discard:
            self._eventbuffer = None
            self._replybuffer = None

    @tulip.coroutine
    def read_event(self):
        """Read the first event in the buffer."""
        if self._eventbuffer is None:
            raise tulip.EofStream("No buffer present.")
        event = yield from self._eventbuffer.read()
        if event is None:
            self._eventbuffer = None
            self.disconnect(False)
            raise tulip.EofStream("EOF encountered while reading events")
        return event

    @tulip.coroutine
    def _read_reply(self):
        """Read the first reply in the buffer."""
        if self._replybuffer is None:
            raise tulip.EofStream("No buffer present.")
        reply = yield from self._replybuffer.read()
        if reply is None:
            self._replybuffer = None
            self.disconnect(False)
            raise tulip.EofStream("EOF encountered while reading replies")
        return reply

    @tulip.coroutine
    def networks(self):
        """Returns a list of active networks on this DaZeus instance."""
        req = dazeus_json_create(dazeus_create_request("get", "networks"))
        reply = yield from self._send(req)
        _check_reply("get", "networks", reply)
        return reply["networks"]

    @tulip.coroutine
    def channels(self, network):
        """Returns a list of channels joined on the specified network."""
        req = dazeus_json_create(dazeus_create_request("get", "channels",
                                                       network))
        reply = yield from self._send(req)
        _check_reply("get", "channels", reply)
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
        _check_reply("do", "message", reply)

    @tulip.coroutine
    def action(self, network, recipient, message):
        """Sends message as CTCP ACTION (/me) to the recipient on the network.

        Recipient may be a channel (usually prefixed with #) or a nickname.
        """
        req = dazeus_json_create(dazeus_create_request("do", "action",
                                                       network, recipient,
                                                       message))
        reply = yield from self._send(req)
        _check_reply("do", "action", reply)

    @tulip.coroutine
    def names(self, network, channel):
        """Sends a NAMES command for the given channel and network.

        If the sending succeeds, a NAMES event will be generated somewhere in
        the future using the DaZeus event system.
        """
        req = dazeus_json_create(dazeus_create_request("do", "names",
                                                       network, channel))
        reply = yield from self._send(req)
        _check_reply("do", "names", reply)

    @tulip.coroutine
    def whois(self, network, nickname):
        """Sends a WHOIS command for the given channel and network.

        If the sending succeeds, a WHOIS event will be generated somewhere in
        the future using the DaZeus event system.
        """
        req = dazeus_json_create(dazeus_create_request("do", "whois",
                                                       network, nickname))
        reply = yield from self._send(req)
        _check_reply("do", "whois", reply)

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
        _check_reply("do", "join", reply)

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
        _check_reply("do", "part", reply)

    @tulip.coroutine
    def nick(self, network):
        """Returns the current nickname for DaZeus on the given network."""
        req = dazeus_json_create(dazeus_create_request("get", "nick",
                                                       network))
        reply = yield from self._send(req)
        _check_reply("get", "nick", reply)
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
        _check_reply("do", "handshake", reply)

    @tulip.coroutine
    def get_config_var(self, vargroup, varname):
        """Retrieves the given variable from the configuration file.

        vargroup can be "core" or "plugin"; "plugin" can only be used after
        performing a successful handshake using `handshake(...)'
        """
        req = dazeus_json_create(dazeus_create_request("get", "config",
                                                       vargroup, varname))
        reply = yield from self._send(req)
        _check_reply("get", "config", reply)
        return reply["value"]

    @tulip.coroutine
    def get_prop(self, propname, network=None, receiver=None, sender=None):
        """Returns the name and value of variable from the persistent database.

        An optional context can be passed in the form of network, receiver and
        sender, in that order of specificity. If multiple properties match,
        only the most specific match will be returned.
        """
        _check_context(network, receiver, sender)
        req = dazeus_json_create(dazeus_create_request("do", "property",
                                                       "get", network,
                                                       receiver, sender))
        reply = yield from self._send(req)
        _check_reply("do", "property", reply)
        return reply["variable"], reply["value"]

    @tulip.coroutine
    def set_prop(self, propname, value, network=None, receiver=None,
                 sender=None):
        """Sets the given variable in the persistent database.

        An optional context can be passed in the form of network, receiver and
        sender, in that order of specificity, so that multiple properties with
        the same name and partially overlapping contexts can be stored. This is
        useful in situations where you want, e.g. different settings per
        network, or different settings per channel.
        """
        _check_context(network, receiver, sender)
        req = dazeus_json_create(dazeus_create_request("do", "property",
                                                       "set", value, network,
                                                       receiver, sender))
        reply = yield from self._send(req)
        _check_reply("do", "property", reply)

    @tulip.coroutine
    def del_prop(self, propname, network=None, receiver=None, sender=None):
        """Deletes the given variable from the persistent database.

        An optional context can be passed in the form of network, receiver and
        sender, in that order of specificity. Only an exact match will be
        removed. If no properties match, no removal will be done, but no error
        will be reported.
        """
        _check_context(network, receiver, sender)
        req = dazeus_json_create(dazeus_create_request("do", "property",
                                                       "unset", network,
                                                       receiver, sender))
        reply = yield from self._send(req)
        _check_reply("do", "property", reply)

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
        _check_context(network, receiver, sender)
        req = dazeus_json_create(dazeus_create_request("do", "property",
                                                       "keys", network,
                                                       receiver, sender))
        reply = yield from self._send(req)
        _check_reply("do", "property", reply)
        return reply["variable"], reply["value"]

    @tulip.coroutine
    def subscribe_events(self, *events):
        """Subscribe to the given events."""
        req = dazeus_json_create(dazeus_create_request("do", "subscribe",
                                                       *events))
        reply = yield from self._send(req)
        _check_reply("do", "subscribe", reply)
        self._subscriptions.update(events)

    @tulip.coroutine
    def unsubscribe_events(self, *events):
        """Unsubscribe from the given events. They will no longer be received,
        unless subscribed to again. Any unprocessed events still waiting to be
        read from the buffer will not be removed.
        """
        req = dazeus_json_create(dazeus_create_request("do", "unsubscribe",
                                                       *events))
        reply = yield from self._send(req)
        _check_reply("do", "unsubscribe", reply)
        self._subscriptions.difference_update(event)

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
        _check_reply("do", "command", reply)

    def subscriptions(self):
        """Returns a set of all events which you are subscribed to."""
        return self._subscriptions

    @tulip.coroutine
    def _send(self, req):
        self._transport.write(req)
        try:
            reply = yield from self._read_reply()
        except tulip.EofStream as exc:
            raise EOFError("Remote closed the connection while we were waiting"
                           " for reply.")
        return reply


def _check_context(network, receiver, sender):
    if sender is not None:
        if receiver is None or network is None:
            raise InvalidContextError()

    if receiver is not None:
        if network is None:
            raise InvalidContextError()


def _check_reply(reqtype, what, reply):
    """Parse the reply and raise appropriate exceptions.

    This function parses a reply for compliance to the DaZeus protocol and
    for success of the request.

    Arguments:
    -reqtype: The type of request for which this reply was generated. Possible
              values: "get" and "do".
    -what: The request-parameter for which this reply was generated, e.g.
           "networks" or "whois". Used to match the value of the "got" or "did"
           fields.
    -reply: The parsed dictionary based on the JSON reply.

    The function can raise various Exceptions based on anomalies encountered:
    -TypeError: if an empty reply is passed in.
    -InvalidReplyError: If a reply is missing the "got" / "did" or "success"
                        fields, or a field does not match its expected value.
    -RequestFailedError: If the "success" field is False.

    If no Exception is raised the function will simply return.
    """
    logging.debug("Reqtype passed to _check_reply: %s", reqtype)
    logging.debug("Reply passed to _check_reply: %s", reply)

    assert reqtype in ("get", "do"), "Invalid reqtype passed to _check_reply"

    if reply is None:
        # This should never happen in the normal flow of the library, since
        # a reply is always obtained through _read_reply which can never return
        # an empty reply.
        # However, not an assert because it still depends on external input.
        logging.error("Empty reply passed to _check_reply")
        raise TypeError("Empty reply passed to _check_reply")

    # TODO make all this case-insensitive, e.g. by casting the appropriate
    # fields to lowercase.
    if reqtype == "get":
        reptype = "got"
    elif reqtype == "do":
        reptype = "did"

    try:
        got = reply[reptype]
    except KeyError:
        #TODO make this warnings instead of exceptions. Will require
        # getting the other possible field before failing.
        logging.error("Invalid reply received: Missing %s", reptype)
        raise InvalidReplyError(
            str.format("Invalid reply received: Missing {}", reptype), reply)

    if got != what:
        logging.error("Invalid reply received: Expected %s but got %s",
                      what, got)
        raise InvalidReplyError(
            str.format("Invalid reply received: Expected {} but got {}",
                       what, got),
            reply)

    try:
        success = reply["success"]
    except KeyError:
        logging.error("Invalid reply received: Missing success")
        raise InvalidReplyError("Invalid reply received: Missing success",
                                reply)

    if not success:
        error = reply.get("error")
        if error is not None:
            logging.error("Request for %s failed with error: %s (Reply: %s)",
                          what, error, reply)
            raise RequestFailedError(error, reply)
        else:
            logging.error("Request for %s failed without error: %s",
                          what, reply)
            raise RequestFailedError("No error message", reply)

    logging.debug("Reply %s for request %s : %s passed validation.", reply,
                  reqtype, what)


def dazeus_json_create(message_dict):
    logging.debug("Creating json from dict: %s", message_dict)
    msg = json.dumps(message_dict, ensure_ascii=False)
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
    assert reqtype in ("do", "get"), logging.warning("Invalid reqtype: %s",
                                                     reqtype)
    msg = {reqtype: what}
    if params is not None:
        assert None not in params, "None passed as parameter to request"
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

    logging.debug("buffer_router encountered EOF. Shutdown.")


class ReplyException(Exception):

    def __init__(self, message, reply):
        Exception.__init__(message)
        self.reply = reply


class RequestFailedError(ReplyException):
    pass


class InvalidReplyError(ReplyException):
    pass


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

    # Call your plugin, defined as a tulip.task, here.

    loop.run_forever()
