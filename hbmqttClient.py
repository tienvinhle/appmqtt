import asyncio
from queueHandler import Message
import asyncio
from hbmqtt.client import MQTTClient, ConnectException
from hbmqtt.mqtt.constants import QOS_1, QOS_2
import json

class HBMQTTClient():
    def __init__(self, deviceID):
        self.mqttClient = MQTTClient()
        self.deviceID = deviceID

    async def connect(self, address, port, user, password):
        await self.mqttClient('mqtt://'+user+':'+password+'@'+address+':'+str(port)+'/')

    async def disconnect(self):
        await self.mqttClient.disconnect()

    def perform(self, object):
        if object is not None:
            jsonObj = json.loads(object)
            loop = asyncio.get_running_loop()
            for (key, value) in jsonObj.items():
                loop.create_task(self.mqttClient.publish('$SYS/'+self.deviceID+'/'+key, value, qos=0x00))


#async def test_coro2():
#    try:
#        C = MQTTClient()
#        await C.connect('mqtt://iot2021:iot2021@113.161.79.146:5000/')
#        await C.publish('$SYS/iot2050No1/temp', b'30', qos=0x00)
#        await C.publish('$SYS/iot2050No1/humidity', b'80', qos=0x01)
#        await C.publish('$SYS/iot2050No1/pressure', b'5', qos=0x02)
#        await C.disconnect()
#    except ConnectException as ce:
#        asyncio.get_event_loop().stop()

async def main():
    mqtt = HBMQTTClient('deviceNo1')
    rd = Message('172.17.0.2', 6379)
    await rd.connect_to_redis()
    rd.add_worker('mqtt', mqtt)
    await rd.add_channel('data')

if __name__ == '__main__':
    asyncio.run(main())
#    asyncio.get_event_loop().run_until_complete(test_coro2())