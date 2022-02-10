import time
import asyncio
import zmq
import zmq.asyncio
context = zmq.asyncio.Context()
socket = context.socket(zmq.PUB)
socket.bind('tcp://*:2000')

# Allow clients to connect before sending data

async def run():
    for i in range(1,2):
        socket.send_pyobj({i:[1,2,3]})
        time.sleep(1)

async def h():
    a = [run()]
    await asyncio.gather(*a)


asyncio.run(h())
