import asyncio
import collections
import logging
import aiofiles

LOGGER = logging.getLogger(__name__)
_QUEUES ={'default': asyncio.Queue(loop=asyncio.get_event_loop())}
#_MESSAGE_QUEUE = asyncio.Queue(loop=asyncio.get_event_loop())

MESSAGE_TYPES = collections.namedtuple(
    'MessageTypes', ('command', 'error', 'response')
)(*('command', 'error', 'response'))
COMMANDS = collections.namedtuple(
    'Commands', ('send', 'read')
)(*('send', 'read'))
PERSISTANCE = collections.namedtuple(
    'QueuePersistance', ('persistant', 'none')
)(*('persistant', 'none'))

@asyncio.coroutine
def handle_command(command, payload,queue):
    LOGGER.debug('Handling command %s, payload %s', command, payload)
    if command not in COMMANDS:
        LOGGER.error('Got invalid command %s', command)
        raise ValueError('Invalid command. Should be one of %s' % (COMMANDS,))
    if command == COMMANDS.send:
        yield from _QUEUES[queue].put(payload)
        msg = 'OK'
    elif command == COMMANDS.read:
        msg = yield from _QUEUES[queue].get()
    return {
        'type': MESSAGE_TYPES.response,
        'payload': msg
    }

@asyncio.coroutine
def dispatch_message(message):
    message_type = message.get('type')
    command = message.get('command')
    queue = message.get('queue')
    if message_type != MESSAGE_TYPES.command:
        LOGGER.error('Got invalid message type %s', message_type)
        raise ValueError('Invalid message type. Should be %s' % (MESSAGE_TYPES.command,))
    if queue not in _QUEUES:
        _QUEUES[queue] = asyncio.Queue(loop=asyncio.get_event_loop())
    LOGGER.debug('Dispatching command %s', command)
    response = yield from handle_command(command, message.get('payload'),queue)
    return response

@asyncio.coroutine
def write_queue(message):
    #Writing persistent queue to disk
    queue = message.get('queue')
    f = yield from aiofiles.open(queue, mode='a')
    try:
        yield from f.write(str(message)+'\n')
    finally:
        yield from f.close()
