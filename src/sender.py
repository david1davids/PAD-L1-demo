#!/usr/bin/env python3
import asyncio
import json
import uuid


@asyncio.coroutine
def send_message(message, loop):
    reader, writer = yield from asyncio.open_connection(
        '127.0.0.1', 14141, loop=loop
    )
    payload = json.dumps({
        'queue': queue_to,
        'type': 'command',
        'command': 'send',
        'persistance': persistance,
        'payload': message
    }).encode('utf-8')

    writer.write(payload)
    writer.write_eof()
    yield from writer.drain()

    response = yield from reader.read(2048)
    writer.close()
    return response


@asyncio.coroutine
def run_sender(loop):
    while True:
        try:
            message = 'Just sending a random UUID %s' % (uuid.uuid4().hex,)
            print('Sending %s' % (message,))
            response = yield from send_message(message, loop)
            message = json.loads(response.decode('utf-8'))
            if message['type'] == 'error':
                print(message['payload'])
                break
            print('Received %s', response)
            yield from asyncio.sleep(1)
        except KeyboardInterrupt:
            break


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_sender(loop))


if __name__ == '__main__':
    p = input('For a persistant queue press 1 ')
    if p:
        if int(p) == 1:
            persistance = True
    else:
        persistance = False
    queue_to = input('\nWhere to send: ')
    main()
