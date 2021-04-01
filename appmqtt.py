import asyncio
from queueHandler import Message
import mqttClient

client_id = 'deviceNo1'
mqttServer = '113.161.79.146'
mqttPort = 5000
mqttUser = 'iot2021'
mqttPassword = 'iot2021'
mqttQoS = 1
mqttKeepAlive = 60

async def main():
    loop = asyncio.get_running_loop()
    mqtt = mqttClient.MQTTClient(client_id, mqttUser, mqttPassword, mqttQoS, loop)
    mqtt.connect('113.161.79.146', 5000, mqttKeepAlive)
    rd = Message('172.17.0.2', 6379)
    await rd.connect_to_redis()
    rd.add_worker('data', mqtt)
    await rd.add_channel('data')

if __name__ == '__main__':
    asyncio.run(main())
#    asyncio.get_event_loop().run_until_complete(test_coro2())