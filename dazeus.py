import collections
import signal
import socket
import os

import json

import tulip

# TODO: cleanup prints
# TODO: fix decent debugging logging
# TODO: documentation
# TODO: well, most of everything else as well.

class DaZeus:

    def __init__(self):
        self._transport = None # FIXME remove this if we never use it directly.
        self._protocol  = None
        
        self._eventbuffer = collections.deque()
        self._replybuffer = collections.deque()

    @tulip.coroutine
    def connect(self, conntype, address, port=None):
        if conntype not in ("tcp", "unix"):
            #FIXME raise exception
            print("Invalid conntype value. Valid values are tcp and unix")
            print(conntype)
            return False

        loop = tulip.get_event_loop()
        #FIXME check for loop == None

        if conntype == "unix":
            usock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            usock.connect(address)
            #FIXME error handling
            transport, protocol = yield from loop.create_connection(tulip.StreamProtocol, sock=usock)
        elif conntype == "tcp":
            transport, protocol = yield from loop.create_connection(tulip.StreamProtocol, address, port)
    
        self._messages = protocol.set_parser(dazeus_message_parser())
        self._transport = transport
        self._protocol  = protocol
    
        return True

    @tulip.coroutine
    def split_stream(self):
        #TODO handle EOF etc.
        """Function that dumps the different DaZeus messages into their correct buffers."""
        messages = self._messages
        if messages is not None:
            message = yield from messages.read()
            if "event" in message:
                self._eventbuffer.feed_data(message)
            else:
                self._replybuffer.feed_data(message)

    @tulip.coroutine
    def _read(self):
        messages = self._messages
        if messages is not None:
            #TODO handle EOF
            return (yield from messages.read())
        else:
            return None

    def _dump_in_buffer(self, message):
        if "event" in message:
            self._eventbuffer.append(message)

    # TODO: create a stream-router which could handle this in a task, then simply
    # yield from the read of each DataBuffer the router writes to.
    @tulip.coroutine
    def read_event(self):
        """Reads messages from the buffer until an event is encountered.

        Any non-events encountered will still be buffered."""
        events = self._eventbuffer
        replies = self._replybuffer
        if not events:
            while True:
                message = yield from self._read()
                if "did" in message or "got" in message:
                    replies.append(message)
                elif "event" not in message:
                    #TODO raise exception
                    return None
                else:
                    return message
        else:
            return events.popleft()

    # TODO: see TODO for read_event()
    @tulip.coroutine
    def _read_reply(self):
        """Reads messages from the buffer until a reply is encountered.

        Any non-replies encountered will still be buffered."""
        events = self._eventbuffer
        replies = self._replybuffer
        if not replies:
            while True:
                message = yield from self._read()
                if "event" in message:
                    events.append(message)
                elif "did" not in message and "got" not in message:
                    #TODO raise exception
                    return None
                else:
                    return message
        else:
            return replies.popleft()

    @tulip.coroutine
    def networks(self):
        """Returns a list of active networks on this DaZeus instance."""
        req = dazeus_json_create(dazeus_create_request_get("networks"))
        self._transport.write(req)
        reply = yield from self._read_reply()
        got = reply.get("got")
        if got != "networks":
            print("INVALID REPLY RECEIVED: {}", reply)
            #TODO make this an exception
            return
        success = reply.get("success")
        if not success:
            print("Getting networks failed: {}", reply)
            #TODO make this an exception
            return
        return reply.get("networks")
        #TODO handle absence of networks.
        
        

    def channels(self, network):
        """Returns a list of channels joined on the specified network."""
        raise NotImplementedError

    def message(self, network, recipient, message):
        """Sends the given message to the given recipient on the network.
        
        Recipient may be a channel (usually prefixed with #) or a nickname."""
        raise NotImplementedError

    def action(self, network, recipient, message):
        """Sends the given message as a CTCP ACTION (/me) to the given recipient on the network.
        
        Recipient may be a channel (usually prefixed with #) or a nickname."""
        raise NotImplementedError

    def names(self, network, channel):
        """Sends a NAMES command for the given channel and network.
        
        If the sending succeeds, a NAMES event will be generated somewhere in the future using 
        the DaZeus event system."""
        raise NotImplementedError

    def whois(self, network, nickname):
        """Sends a WHOIS command for the given channel and network.
        
        If the sending succeeds, a WHOIS event will be generated somewhere in the future using 
        the DaZeus event system."""
        raise NotImplementedError

    def join(self, network, channel):
        """Sends a JOIN command for the given channel and network.
        
        If the sending succeeds and the join is successful, a JOIN event will be generated 
        somewhere in the future using the DaZeus event system. A join is not successful if the
        channel was already joined."""
        raise NotImplementedError
    
    def part(self, network, channel):
        """Sends a PART command for the given channel and network.
        
        If the sending succeeds and the part is successful, a PART event will be generated 
        somewhere in the future using the DaZeus event system. A part is not successful if the
        channel was not joined."""
        raise NotImplementedError

    def nick(self, network):
        """Returns the current nickname for DaZeus on the given network."""
        raise NotImplementedError

    def handshake(self, name, version, configname=None):
        """Performs the (optional) DaZeus handshake.

        The handshake is required for receiving plugin configuration.
        If configname is not given, name is used."""
        if configname is None:
            configname = name

        raise NotImplementedError
    
    def get_config_var(self, vargroup, varname):
        """Retrieves the given variable from the configuration file.

        vargroup can be "core" or "plugin"; "plugin" can only be used after performing a
        successful handshake using `handshake(...)'"""
        raise NotImplementedError

    def get_prop(self, propname, network=None, receiver=None, sender=None):
        """Retrieves the given variable from the persistent database and returns its value.

        An optional context can be raise NotImplementedErrored in the form of network, receiver and sender, in that
        order of specificity. If multiple properties match, only the most specific match will
        be returned."""
        raise NotImplementedError

    def set_prop(self, propname, network=None, receiver=None, sender=None):
        """Sets the given variable in the persistent database.

        An optional context can be raise NotImplementedErrored in the form of network, receiver and sender, in that
        order of specificity, so that multiple properties with the same name and partially 
        overlapping contexts can be stored. This is useful in situations where you want, e.g.
        different settings per network, or different settings per channel."""
        raise NotImplementedError
        #TODO what the hell does the perl code do here?

    def del_prop(self, propname, network=None, receiver=None, sender=None):
        """Deletes the given variable from the persistent database.

        An optional context can be raise NotImplementedErrored in the form of network, receiver and sender, in that
        order of specificity. If multiple properties match, only the most specific match will
        be removed. If not properties match, no removal will be done."""
        # FIXME: Only the most specific, or only an exact match?
        raise NotImplementedError

    def get_prop_keys(self, namespace, network=None, receiver=None, sender=None):
        """Retrieves all keys in a given namespace.

        E.g. if example.foo and example.bar were stored earlier, get_prop_keys("example") will
        return ["foo", "bar"].
        An optional context can be raise NotImplementedErrored in the form of network, receiver and sender, in that
        order of specificity. If multiple namespaces match, only the most specific match will
        be returned."""
        raise NotImplementedError

    def subscribe_events(self, events):
        """Subscribe to the given events."""
        raise NotImplementedError

    def unsubscribe_events(self, events):
        """Unsubscribe from the given events. They will no longer be received, unless subscribed
        to again. Any unprocessed events still waiting to be read from the buffer will not be
        removed."""
        raise NotImplementedError

    def subscriptions(self):
        """Returns a list of all events which you are subscribed to."""
        raise NotImplementedError



def create_json(json_dict):
    print(json_dict)

    json_raw = json.dumps(json_dict)
    print(json_raw)

    return json_raw

def dazeus_json_create(message_dict):
    msg = create_json(message_dict)
    print(msg)
    msg = msg.encode('utf-8')
    print(msg)
    print(len(msg))
    print(str(len(msg)))

    length = str(len(msg))
    length = length.encode('utf-8')
    msg = length + msg
    print(msg)

    return msg

def dazeus_create_request_do(dotype, params=None):
    msg = { "do" : dotype }
    if params is not None:
        msg["params"] = params
    return msg

def dazeus_create_request_get(gettype, params=None):
    msg = { "get" : gettype }
    if params is not None:
        msg["params"] = params
    return msg

def parse_json(json_raw):
    print(json_raw)
    # FIXME remove print

    json_dict = json.loads(json_raw)
    print(json_dict)
    
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
            # read the size of the JSON message, at most 20 bytes of ASCII-digits
            # buf.readuntil also yields the delimiting character, so strip that.
            json_size = yield from buf.readuntil(b'{', 20, BufferError)
            print(json_size)
            json_size = json_size.decode('ascii')[:-1]
            print(json_size)
            
            if not json_size.isdecimal():
                json_size = json_size.lstrip("\r\n")
    
            #TODO better handling for invalid characters in stream.
            json_size = int(json_size)
            print(json_size)
            #FIXME remove prints
            
            # read the JSON message itself, terminating at the delimiting character
            # or at json_size. FIXME throw an exception if the delimiter is not found
            # at json_size.
            json_raw = b'{' + (yield from buf.read(json_size - 1))
            json_raw = json_raw.decode('utf-8')
            if json_raw[-1] != '}':
                print("} not found at expected location")

            # Parse the raw JSON
            # TODO: invalid json handling
            json_dict = parse_json(json_raw)

            # We can now push the JSON dictionary onto the application's databuffer.
            out.feed_data(json_dict)
    except:
        print("Exception caught while parsing protocol messages")
        raise



@tulip.task
def networkzeus():
    dazeus = DaZeus()
#   connection = yield from dazeus.connect("unix", "/tmp/pythonzeustest.sock")
    connection = yield from dazeus.connect("tcp", "localhost", 1234)
    print(connection)

    while(True):
        networks = yield from dazeus.networks()
        print(networks)



if __name__ == "__main__":
    # Setup the event loop
    loop = tulip.get_event_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, loop.stop)
    except RuntimeError:
        print("RuntimeError caught when trying to add signal handler to event loop")

    networkzeus()

    loop.run_forever()

