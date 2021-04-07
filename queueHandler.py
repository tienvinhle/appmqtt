import asyncio
import aioredis
import json
from collections import OrderedDict

class Message(object):
    def __init__(self, redisHost, redisPort):
        self.redisHost = redisHost
        self.redisPort = redisPort
        self.worker = None
        self.redis = None
        self.channels = []

    def add_worker(self, workerInstance):
        self.worker = workerInstance

    async def connect_to_redis(self):
        conn = 'redis://' + self.redisHost + ':' + str(self.redisPort)
        print(conn)
        self.redis = await aioredis.create_redis(conn)

    async def send_message(self, channel, message):
        await self.redis.publish(channel, message)

    async def add_channel(self, *channels):
        if len(channels)>0:
            for channel in channels:
                tsk = None
                if ('*' not in channel):
                    res = await self.redis.subscribe(channel)
                    self.channels.append(res[0])
                    tsk = asyncio.ensure_future(self.callback_message_comes(res[0]))
                else:
                    pat = await self.redis.psubscribe(channel)
                    tsk = asyncio.ensure_future(self.callback_message_comes(pat[0]))
                await tsk
    
    async def callback_message_comes(self, channel):
        while (await channel.wait_message()):
            msg = await channel.get_json()
            print("Got Message: from channel", msg)
            msgSend = dict()
            if (type(msg) == list) | (type(msg) == tuple):
                print('Tuple or List')
                msgSend[msg[1].name.decode()] = msg[2]
            else:
                print('single object')
                #decode to convert to string from binary b'
                msgSend[channel.name.decode()] = msg
            self.worker.perform(msgSend)
