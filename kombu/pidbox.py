import logging
import socket

from UserDict import UserDict

from kombu import entity
from kombu import messaging

PREFIX_FMT = "mb%s"


"""

==================================
 pidbox - Mail box for processes.
==================================

Server
======

mailbox = pidbox.Mailbox("celerybeat", type="direct")

@mailbox.handler
def reload_schedule(state, **kwargs):
    beat.reload_schedule()
    state["last_reload"] = time()

@mailbox.handler
def connection_info(state, **kwargs):
    return {"connection": connection.info()}


connection = kombu.BrokerConnection()
channel = connection.channel()
mailbox.listen(channel) / mailbox.listen(channel, hostname="me.example.com")



Client
======

celerybeat = pidbox.lookup("celerybeat")
celerybeat.cast("reload_schedule")
info = celerybeat.call("connection_info", timeout=10)

"""


class Mailbox(object):
    Exchange = entity.Exchange
    Queue = entity.Queue
    Consumer = messaging.Consumer

    version = 0

    type_aliases = {"broadcast": "fanout"}

    def __init__(self, ident, state=None, type="direct", handlers=None):
        self.ident = ident
        self.state = state or {}
        self.type = type
        if handlers is None:
            handlers = {}
        self.handlers = handlers

        self.exchange = self.Exchange(name=self.prefix + self.ident,
                                      type=self.exctype(self.type),
                                      durable=False,
                                      delivery_mode="transient",
                                      auto_delete=True)

    def exctype(self, type):
        return self.type_aliases.get(type, type)

    def clone(self, state):
        return self.__class__(ident=self.ident,
                              state=state,
                              type=self.type,
                              handlers=self.handlers)

    def handle(self, method, arguments):
        handler = self.handlers[method]
        ret = handler(self.state, arguments)
        if len(ret) > 1:
            self.state, ret = ret
            return ret
        self.state = ret

    def handle_call(self, method, arguments):
        return self.handle(method, arguments)

    def handle_cast(self, method, arguments):
        return self.handle(method, arguments)

    def handler(self, fun):
        self.handlers[fun.__name__] = fun
        return fun

    def queue(self, hostname):
        return self.Queue(name=self.prefix + hostname,
                          exchange=self.exchange,
                          auto_delete=True,
                          durable=False)

    def reply(self, reply, exchange, routing_key):
        channel = self.channel
        crq = self.ReplyProducer(channel, exchange=exchange)
        crq.publish(reply, routing_key=routing_key)

    def on_message(self, message_data, message):
        self.dispatch_from_message(message)

    def dispatch_from_message(self, message):
        message = dict(message) # Don't modify callers message
        method = message["method"]
        destination = message.get("destination")
        reply_to = message.get("reply_to")
        arguments = message["arguments"]
        if not destination or self.hostname in destination:
            return self.dispatch(method, arguments=message, reply_to=reply_to)

    def dispatch(self, method, arguments=None, reply_to=None):
        arguments = arguments or {}
        handle = reply_to and self.handle_call or self.handle_cast
        try:
            reply = handle(method, arguments)
        except SystemExit:
            raise
        except Exception, exc:
            reply = {"error": repr(exc)}

        if reply_to:
            self.reply({self.hostname: reply},
                        exchange=reply_to["exchange"],
                        routing_key=reply_to["routing_key"])
        return reply

    def listen(self, channel, hostname=None, callback=None):
        hostname = hostname or socket.gethostname()
        queue = self.queue(hostname)
        consumer = self.Consumer(channel, queue)
        consumer.register_callback(callback or self.on_message)
        consumer.consume()
        return consumer

    @property
    def prefix(self):
        return PREFIX_FMT % self.version


class ReplyExchange(entity.Exchange):
    name = "mb.replies"
    type = "direct"
    durable = False
    auto_delete = True
    delivery_mode = "transient"


class ReplyQueue(entity.Queue):
    exchange = ReplyExchange()
    durable = False
    auto_delete = True

    def __init__(self, ticket, *args, **kwargs):
        self.ticket = ticket
        kwargs["queue"] = "%s.%s" % (self.exchange.name, self.ticket)
        super(ReplyQueue, self).__init__(*args, **kwargs)


class Dispatcher(object):
    Consumer = messaging.Consumer
    Producer = messaging.Producer

    _consumer = None

    def __init__(self, mailbox, connection, reply_queue=None):
        self.mailbox = mailbox
        self.connection = connection
        self.channel = connection.channel()
        self.reply_queue = reply_queue or ReplyQueue()
        self._responses = deque()

    def prepare(self, message, arguments, destination=None,
            reply_ticket=None):
        if destination is not None and \
                not isinstance(destination, (list, tuple)):
            raise ValueError("destination must be a list/tuple not %s" % (
                    type(destination)))
        payload = {"method": message,
                   "arguments": arguments,
                   "destination": destination}
        if reply_ticket:
            payload["reply_to"] = {"exchange": self.reply_queue.exchange.name,
                                   "routing_key": reply_ticket}
        return payload

    def send_call(self, message, arguments, destination=None,
            reply_ticket=None):
        self.producer.publish(self.prepare(message, arguments, destination,
                                           reply_ticket))
        return reply_ticket


    def call(self, message, arguments, destination=None, reply_ticket=None,
            limit=1, timeout=None):
        reply_ticket = self.send_call(message, arguments, destination,
                                      reply_ticket)
        # Set reply limit to number of destinations (if specificed)
        if limit is None and destination:
            limit = destination and len(destination) or None
        responses = []
        for i in count(0):
            if i > limit:
                return responses
            responses.append(self.collect


    def cast(self, message, arguments, destination=None):
        self.producer.publish(self.prepare(message, arguments, destination))

    def collect(self, reply_ticket, timeout=1):
        while 1:
            try:
                return self._responses.popleft()
            except IndexError:
                self.connection.drain_events(timeout=timeout)

    def on_reply(self, message_data, message):
        self._responses.append(message)

    def get_consumer(self, reply_ticket):
        consumer = self.Consumer(self.channel, self.reply_queue)
        consumer.register_callback(self.on_reply)
        consumer.consume()
        return self._consumer

    @property
    def producer(self):
        if self._producer is None:
            self._producer = self.Producer(self.channel, self.mailbox.exchange)
