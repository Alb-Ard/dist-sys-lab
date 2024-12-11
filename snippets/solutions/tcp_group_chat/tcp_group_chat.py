from datetime import datetime
from collections.abc import Callable
from threading import Thread
from typing import Optional, Tuple
import ipaddress
import socket
import sys
import time

MESSAGE_TYPE_TEXT = 0
MESSAGE_TYPE_ROUTE = 1

class MessageParser:
    text_received_callback: Callable[[(str, int), str], None]
    route_received_callback: Callable[[(str, int), list[(str, int)]], None]

    def receive(self, source: (str, int), producer: Callable[[int], bytes]) -> bool:
        message_type = int.from_bytes(producer(4))
        if message_type == MESSAGE_TYPE_TEXT:
            text_length = int.from_bytes(producer(4))
            text = producer(text_length).decode()
            self.text_received_callback(source, text)
            return True
        elif message_type == MESSAGE_TYPE_ROUTE:
            route_count = int.from_bytes(producer(4))
            routes = list[(str, int)]()
            for i in range(route_count):
                address = ipaddress.IPv4Address(producer(4)).exploded
                port = int.from_bytes(producer(4))
                routes.append((address, port))
            self.route_received_callback(source, routes)
            return True
        return False

class MessageSerializer:
    def send_text(self, text: str, consumer: Callable[[bytes], None]):
        print(f"Sending text \"{text}\" to all")
        consumer(MESSAGE_TYPE_TEXT.to_bytes(4))
        encoded_text = text.encode()
        consumer(len(encoded_text).to_bytes(4))
        consumer(encoded_text)

    def send_route_to(self, routes: list[(str, int)], target: (str, int), consumer: Callable[[(str, int), bytes], None]):
        print(f"Sending routes to {target[0]}:{target[1]}")
        consumer(MESSAGE_TYPE_ROUTE.to_bytes(4), target)
        consumer(len(routes).to_bytes(4), target)
        for route in routes:
            packed_address = ipaddress.IPv4Address(route[0]).packed
            consumer(packed_address, target)
            consumer(route[1].to_bytes(4), target)

class ChatReceiver:
    client_disconnected_callback: Callable[[(str, int)], None]

    __listen_thread: Thread
    __client_threads = dict[(str, int), (Thread, socket.socket)]()
    __data_parser: MessageParser

    def __init__(self, bind_host: str, bind_port: int, data_parser: MessageParser):
        self.__listen_thread = Thread(target=lambda: self.__listen(bind_host, bind_port), daemon=True)
        self.__data_parser = data_parser

    def start(self) -> bool:
        if self.is_running():
            return False
        self.__listen_thread.start()
        return True

    def disconnect(self, host: str, port: int):
        target = (host, port)
        if target in self.__client_threads.keys():
            self.__client_threads[target][1].close()
            self.__client_threads.pop(target)

    def is_running(self) -> bool:
        return self.__listen_thread.is_alive()

    def __listen(self, bind_host: str, bind_port: int):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((bind_host, bind_port))
        server_socket.listen(1)
        while True:
            print(f"Listening on {bind_host}:{bind_port}")
            (receive_socket, client_endpoint) = server_socket.accept()
            client_thread = Thread(target=lambda: self.__receive(receive_socket), daemon=True)
            client_thread.start()
            self.__client_threads[client_endpoint] = (client_thread, receive_socket)

    def __receive(self, client_socket: socket.socket):
        (client_host, client_port) = client_socket.getpeername()
        try:
            print(f"Client connected from {client_host}:{client_port}")
            while self.is_running():
                self.__data_parser.receive((client_host, client_port), lambda size: client_socket.recv(size))
            print(f"Disconnecting from {client_host}:{client_port}")
            client_socket.close()
        except ConnectionError:
            print(f"Connection error, disconnected from {client_host}:{client_port}")
        #except Exception as ex:
        #    print(f"Error: {ex}, disconnected from {client_host}:{client_port}")
        self.__client_threads.pop((client_host, client_port))
        self.client_disconnected_callback((client_host, client_port))

class ChatClient:
    client_disconnected_callback: Callable[[(str, int)], None]

    __send_sockets: dict[(str, int), socket.socket] = dict()

    def connect(self, host: str, port: int, connected_callback: Callable[[(str, int)], None]):
        Thread(target=lambda: self.__connect(host, port, connected_callback), daemon=True).start()

    def disconnect(self, host: str, port: int):
        target = (host, port)
        if target in self.__send_sockets.keys():
            self.__send_sockets[target].close()
            self.__remove_client(target)

    def send(self, data: bytes):
        print(f"Broadcasting data")
        sockets = set(self.__send_sockets.values())
        for send_socket in sockets:
            self.__send_to_socket(data, send_socket)

    def send_to(self, data: bytes, target: (str, int)):
        print(f"Sending data to {target[0]}:{target[1]}")
        self.__send_to_socket(data, self.__send_sockets[target])

    def get_routes(self) -> set[(str, int)]:
        return set(self.__send_sockets.keys())

    def __send_to_socket(self, data: bytes, target_socket: socket):
        (client_host, client_port) = target_socket.getpeername()
        try:
            print(f"Sending data to socket on {client_host}:{client_port}")
            target_socket.send(data)
        except ConnectionError:
            print(f"Connection error")
            self.__remove_client((client_host, client_port))
        except Exception as ex:
            print(f"Error: {ex} sending to {client_host}:{client_port}")

    def __connect(self, host: str, port: int, connected_callback: Callable[[(str, int)], None]):
        if (host, port) in self.__send_sockets:
            print(f"Already connected to {host}:{port}, skipping...")
            return
        while True:
            try:
                print(f"Connecting to {host}:{port}")
                send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                send_socket.connect((host, port))
                self.__send_sockets[(host, port)] = send_socket
                print(f"Connected to {host}:{port}")
                connected_callback((host, port))
                return
            except ConnectionError:
                print(f"Connection error")
                self.__remove_client((host, port))
    
    def __remove_client(self, endpoint: (str, int)):
        self.__send_sockets.pop(endpoint)
        print(f"Removed client {endpoint[0]}:{endpoint[1]}")
        self.client_disconnected_callback(endpoint)

class ChatManager:
    message_received: Callable[[(str, int), str], None]

    __listen_endpoint: (str, int)
    __client: ChatClient
    __serializer: MessageSerializer
    __parser: MessageParser
    __receiver: ChatReceiver
    # Pairs of remote-endpoint <-> local-endpoint
    __endpoint_pairs = set[((str, int), (str, int))]()

    def __init__(self, listen_endpoint: (str, int)):
        self.__listen_endpoint = listen_endpoint
        self.__parser = MessageParser()
        self.__serializer = MessageSerializer()
        self.__client = ChatClient()
        self.__receiver = ChatReceiver(listen_endpoint[0], listen_endpoint[1], self.__parser)
        
        self.__parser.text_received_callback = self.__handle_text_message
        self.__parser.route_received_callback = lambda source, routes: self.__handle_route_message(source, routes)
        self.__client.client_disconnected_callback = lambda target: self.__disconnect_from(self.__get_endpoints_from_remote(target))
        self.__receiver.client_disconnected_callback = lambda target: self.__disconnect_from(self.__get_endpoints_from_local(target))

    def start(self, initial_connections: set[(str, int)]) -> bool:
        if not self.__receiver.start():
            return False
        for connection in initial_connections:
            self.__connect_to((connection[0], connection[1]))
        return True

    def send_message(self, message: str):
        self.__serializer.send_text(message, self.__client.send)

    def __disconnect_from(self, endpoints: list[(str, int)]):
        if len(endpoints) < 1:
            return
        self.__endpoint_pairs.remove((endpoints[0], endpoints[1]))
        self.__client.disconnect(endpoints[0][0], endpoints[0][1])
        self.__receiver.disconnect(endpoints[1][0], endpoints[1][1])

    def __get_endpoints_from_remote(self, remote_endpoint: (str, int)) -> list[(str, int)]:
        for eps in self.__endpoint_pairs:
            if eps[0] == remote_endpoint:
                return list(eps)
        return []

    def __get_endpoints_from_local(self, local_endpoint: (str, int)) -> list[(str, int)]:
        for eps in self.__endpoint_pairs:
            if eps[1] == local_endpoint:
                return list(eps)
        return []

    # Connects to a client and, if this is a new connection, sends the route table to him
    def __connect_to(self, target: (str, int)):
        self.__client.connect(target[0], target[1], lambda target: self.__send_route_to_client(target))

    def __send_route_to_client(self, target: (str, int)):
        routes = list(self.__client.get_routes())
        # The last route is myself
        routes.append(self.__listen_endpoint)
        self.__serializer.send_route_to(routes, target, self.__client.send_to)

    def __handle_text_message(self, source: (str, int), text: str):
        self.message_received(source, text)

    def __handle_route_message(self, source: (str, int), routes: list[(str, int)]):
        routes.remove(self.__listen_endpoint)
        for route in routes:
            print(f"Received route to {route[0]}:{route[1]}")
            self.__connect_to((route[0], route[1]))
        # The last route is the source client's one
        routeList = list(routes)
        self.__endpoint_pairs.add(((routeList[len(routeList) - 1]), source))

def __format_message(text: str, sender: str, timestamp: datetime=None):
    if timestamp is None:
        timestamp = datetime.now()
    return f"[{timestamp.isoformat()}] {sender}:\n\t{text}"

def main():
    if len(sys.argv) < 2:
        print("Usage: python tcp_group_chat.py <bind address> <bind port> [<peer 1 address> <peer 1 port>] [<peer 2 address> <peer 2 port>] ...")
        return

    listen_endpoint = (sys.argv[1], int(sys.argv[2]))
    chat = ChatManager(listen_endpoint)
    chat.message_received = lambda source, text: print(__format_message(text, f"{source[0]}:{source[1]}"))
    connections = set([(sys.argv[i], int(sys.argv[i + 1])) for i in range(3, len(sys.argv), 2)])

    if not chat.start(connections):
        print("Could not start client!")
        return
    try:
        while True:
            text = input("> ")
            chat.send_message(text)
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()