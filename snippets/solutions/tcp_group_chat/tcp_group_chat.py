from typing import TypeVar, Generic
from datetime import datetime
from collections.abc import Callable
from threading import Thread
import socket
import sys
import time

class ChatClient:
    __receive_thread: Thread
    __send_socket: socket.socket
    __text_received_callback: Callable[[str], None]
    __is_connected = False

    def __init__(self, bind_host: str, bind_port: int, text_received_callback: Callable[[str], None]):
        self.__listen_thread = Thread(target=lambda: self.__listen(bind_host, bind_port), daemon=True)
        self.__text_received_callback = text_received_callback

    def start(self, host: str, port: int) -> bool:
        if self.is_running():
            return False
        self.__listen_thread.start()
        Thread(target=lambda: self.__connect(host, port)).start()
        return True

    def send_text(self, text: str):
        encoded_text = text.encode()
        self.__send_socket.send(len(encoded_text).to_bytes(4))
        self.__send_socket.send(encoded_text)

    def is_running(self) -> bool:
        return self.__listen_thread.is_alive()
    
    def is_connected(self) -> bool:
        return self.__is_connected

    def __connect(self, host: str, port: int):
        while self.is_running():
            try:
                print(f"Connecting to {host}:{port}")
                self.__send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.__send_socket.connect((host, port))
                self.__is_connected = True
                print(f"Connected to {host}:{port}")
                return
            except:
                print(f"Connection error")

    def __listen(self, bind_host: str, bind_port: int):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((bind_host, bind_port))
        server_socket.listen(1)
        while True:
            print(f"Listening on {bind_host}:{bind_port}")
            (receive_socket, client_address) = server_socket.accept()
            print(f"Client connected from {client_address}")
            try:
                self.__receive(receive_socket)
            except ConnectionError:
                self.__send_socket.close()
                self.__is_connected = False
                print("Connection error, disconnected")

    def __receive(self, client_socket: socket.socket):
        while True:
            text_length = int.from_bytes(client_socket.recv(4))
            if text_length == 0:
                return
            text = client_socket.recv(text_length).decode()
            self.__text_received_callback(text)

def __format_message(text: str, sender: str, timestamp: datetime=None):
    if timestamp is None:
        timestamp = datetime.now()
    return f"[{timestamp.isoformat()}] {sender}:\n\t{text}"

def main():
    client = ChatClient(sys.argv[1], int(sys.argv[2]), lambda text: print(f"Received {text}", end="\n> "))
    try:
        while True:
            if not client.start(sys.argv[3], int(sys.argv[4])):
                print("Could not start client!")
                time.sleep(0.5)
                continue
            while not client.is_connected():
                time.sleep(0.5)
            while client.is_connected():
                text = input("> ")
                client.send_text(text)
                time.sleep(0.1)
    except KeyboardInterrupt:
        pass
    except:
        print("Fatal error!")

if __name__ == "__main__":
    main()