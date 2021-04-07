import asyncio
from queueHandler import Message
import mqttClient

configPath = "/etc/mqtt/appConfig.conf"
#orgID will be read from central config deliveried from Agent via Redis GET
orgID = "mrViet"
#device_id will be read from central config deliveried from Agent via Redis GET
device_id = 'deviceNo1'
client_id = device_id

def getConfig(configPath):
	with open(configPath, 'r') as f:
			config = json.load(f, object_pairs_hook=OrderedDict)
			return config

async def main():
    conf = getConfig(configPath)
    mqtt = mqttClient.MQTTClient(client_id, orgID, conf["mqtt"]["user"], conf["mqtt"]["password"], int(conf["mqtt"]["QoS"]))
    mqtt.connect(conf["mqtt"]["ip"], int(conf["mqtt"]["port"]), conf["mqtt"]["keepAliveTime"])
    mqtt.client.loop_start()

    rd = Message(conf["micro"]["ip"], int(conf["micro"]["port"]))
    await rd.connect_to_redis()
    rd.add_worker(mqtt)
    format2Sub = conf["micro"]["subscription"]["format"]
    data2Sub = conf["micro"]["subscription"]["data2Sub"]
    tempChannel = None
    for item in data2Sub:
        tempChannel = format2Sub.replace("<thingID>", item["thingID"])
        tempChannel = tempChannel.replace("<datapoint>", item["datapoint"])
        print('Redis Client about to subsribe to {} channel', tempChannel)
        await rd.add_channel(tempChannel)

if __name__ == '__main__':
    asyncio.run(main())
#    asyncio.get_event_loop().run_until_complete(test_coro2())