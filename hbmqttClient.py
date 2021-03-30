import asyncio
from queueHandler import Message

from hbmqtt.client import MQTTClient, ConnectException
from hbmqtt.mqtt.constants import QOS_1, QOS_2

#
# This sample shows how to publish messages to broker using different QOS
# Debug outputs shows the message flows
#
@asyncio.coroutine
def test_coro():
    C = MQTTClient()
    yield from C.connect('mqtt://113.161.79.146:5000/')
    tasks = [
        asyncio.ensure_future(C.publish('a/b', b'TEST MESSAGE WITH QOS_0')),
        asyncio.ensure_future(C.publish('a/b', b'TEST MESSAGE WITH QOS_1', qos=QOS_1)),
        asyncio.ensure_future(C.publish('a/b', b'TEST MESSAGE WITH QOS_2', qos=QOS_2)),
    ]
    yield from asyncio.wait(tasks)
    yield from C.disconnect()


@asyncio.coroutine
def test_coro2():
    try:
        C = MQTTClient()
        yield from C.connect('mqtt://iot2021:iot2021@113.161.79.146:5000/')
        yield from C.publish('$SYS/iot2050No1/temp', b'30', qos=0x00)
        yield from C.publish('$SYS/iot2050No1/humidity', b'80', qos=0x01)
        yield from C.publish('$SYS/iot2050No1/pressure', b'5', qos=0x02)
        yield from C.disconnect()
    except ConnectException as ce:
        asyncio.get_event_loop().stop()


if __name__ == '__main__':
#    asyncio.get_event_loop().run_until_complete(test_coro())
    asyncio.get_event_loop().run_until_complete(test_coro2())