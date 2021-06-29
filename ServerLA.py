#!/usr/bin/python3

import time
import logging
import configparser
from _thread import *
import threading
import socket
from ast import literal_eval
from model import Model
import struct
import pickle
import json
import ssl

class ServerLA():

    def __init__(self):

        logging.basicConfig(
            level=logging.DEBUG,
            format='[ServerLA] %(asctime)s %(levelname)s %(message)s',
            filename='/var/log/ServerLA.log')

        self.config = configparser.ConfigParser()
        self.config.read('config.ini')

        self.model = Model()

        self.lost_conn = False

        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_cert_chain(certfile=r"ssl/server.crt", keyfile=r"ssl/server.key")
        context.load_verify_locations(cafile=r"ssl/client/client.crt")

        sock = socket.socket()
        self.conn = context.wrap_socket(sock, server_side=True)
        self.conn.bind(('', int(self.config['server']['port'])))
        self.conn.listen(1)
        #self.conn.settimeout(10.0)
        self.lock = threading.Lock()

    def multi_threaded_client(self, connection, lock, is_alive):

        print(connection.getpeercert())
        while is_alive:
            print(is_alive)
            print(time.ctime(), get_ident(), sep="----")
            try:

                data_size = struct.unpack('>I', connection.recv(4))[0]
                received_payload = b""
                reamining_payload_size = data_size
                while reamining_payload_size != 0:
                    received_payload += connection.recv(reamining_payload_size)
                    reamining_payload_size = data_size - len(received_payload)
                data = pickle.loads(received_payload)
            except:
                #data = dict()

                if not data:
                    break

            try:

                response = self.model.insert_data(data)
                print(data)
            except Exception as e:
                response = ''
                print(22222222222, e, sep="-----")

            try:
                print(4444444)
                
                connection.sendall(struct.pack('>I', len(pickle.dumps(response))))
                connection.sendall(pickle.dumps(response))
            except Exception as e:
                print(33333333333, e, sep="----")
                #connection.close()
                #is_alive = False
                #time.sleep(3)
                #self.lost_conn = True
                exit_thread()




    def main(self):
        thead_count = 0
        while True:
            """
            if self.lost_conn:
                context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                context.verify_mode = ssl.CERT_REQUIRED
                context.load_cert_chain(certfile=r"ssl/server.crt", keyfile=r"ssl/server.key")
                context.load_verify_locations(cafile=r"ssl/client/client.crt")
                sock = socket.socket()
                self.conn = context.wrap_socket(sock, server_side=True)
                self.conn.bind(('', int(self.config['server']['port'])))
                self.conn.listen(1)
                self.conn.settimeout(10.0)
                """

            try:
                Client, address = self.conn.accept()
                logging.info('Connected to: ' + address[0] + ':' + str(address[1]))
                accept = threading.Thread(target=self.multi_threaded_client, args=(Client, self.lock, True))
                accept.start()
                thead_count += 1
                logging.info('Thread Number: ' + str(thead_count))
            except:
                #self.lost_conn = True
                #self.conn.close()
                print("except, connect close in while")
        conn.close()


if __name__ == '__main__':
    server = ServerLA()
    server.main()



