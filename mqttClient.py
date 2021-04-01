import socket
import paho.mqtt.client as mqtt
import asyncio

class AsyncioHelper:
    def __init__(self, loop, client):
        self.loop = loop
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write

    def on_socket_open(self, client, userdata, sock):
        print("Socket opened")

        def cb():
            print("Socket is readable, calling loop_read")
            client.loop_read()

        self.loop.add_reader(sock, cb)
        self.misc = self.loop.create_task(self.misc_loop())

    def on_socket_close(self, client, userdata, sock):
        print("Socket closed")
        self.loop.remove_reader(sock)
        self.misc.cancel()

    def on_socket_register_write(self, client, userdata, sock):
        print("Watching socket for writability.")

        def cb():
            print("Socket is writable, calling loop_write")
            client.loop_write()

        self.loop.add_writer(sock, cb)

    def on_socket_unregister_write(self, client, userdata, sock):
        print("Stop watching socket for writability.")
        self.loop.remove_writer(sock)

    async def misc_loop(self):
        print("misc_loop started")
        while self.client.loop_misc() == mqtt.MQTT_ERR_SUCCESS:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
        print("misc_loop finished")

class MQTTClient:
    def __init__(self, clientID, user, password, qos, loop):
        self.clientID = clientID
        self.loop = loop
        self.user = user
        self.password = password
        self.qos = qos
        self.client = mqtt.Client(client_id=clientID)
        self.client.username_pw_set(user, password)
        self.aioh = AsyncioHelper(self.loop, self.client)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
#        self.client.on_disconnect = self.on_disconnect

    def connect(self, host, port, keepalive):
        self.client.connect(host, port, keepalive)
        self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

    def disconnect(self):
        self.client.disconnect()

    def on_connect(self, client, userdata, flags, rc):
        print("Subscribing")
        client.subscribe(topic)

    def on_message(self, client, userdata, msg):
        if not self.got_message:
            print("Got unexpected message: {}".format(msg.decode()))
        else:
            self.got_message.set_result(msg.payload)

#    def on_disconnect(self, client, userdata, rc):
#        self.disconnected.set_result(rc)

    def publish_msg(self, topic, message, QoS):
        encoded_msg = str(message).encode()
        self.client.publish(topic, encoded_msg, QoS)

    def perform(self, obj):
        if obj['type'] is not None:
            if obj['type'] == 'mqtt':
                if obj['function'] == 'publish':
                    for (topic, msg) in obj['para'].items():
                        self.publish_msg(topic, msg, self.qos)