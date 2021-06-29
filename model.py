import psycopg2
import time
import configparser
import logging
import re
import json


class Model(object):


    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Model, cls).__new__(cls)
        return cls._instance

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read('config.ini', encoding='utf-8')

        logging.basicConfig(
            level=logging.DEBUG,
            format='[AgentLA] %(asctime)s %(levelname)s %(message)s',
            filename='/var/log/ServerLA.log'
        )

        try:
            self.conn = psycopg2.connect(database=self.config['database']['dbname'],
                                         user=self.config['database']['username'],
                                         password=self.config['database']['password'],
                                         host=self.config['database']['hostname'], port=self.config['database']['port'])
            self.cursor = self.conn.cursor()
        except IndexError:
            logging.error(IndexError)

    def __del__(self):
        if self.cursor:
            self.cursor.close()

    def get_server_id(self, name_id):
        self.cursor.execute('SELECT id FROM servers WHERE name_id = %s', (name_id,))
        return self.cursor.fetchone()[0]

    def insert_log_auth(self, args, server_id):

        if bool(args):
            try:
                self.cursor.execute('SELECT record FROM log_auth WHERE server_id = %s ORDER BY record DESC LIMIT 1 ', (server_id,))
                last_record = self.cursor.fetchone()[0]
            except:
                for log in args:
                    self.cursor.execute("INSERT INTO log_auth("
                                        "time, "
                                        "event_id, "
                                        "record, "
                                        "username, "
                                        "address, "
                                        "hostname, "
                                        "point, "
                                        "domain, "
                                        "server_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                         (log["time"],
                                         log["event_id"],
                                         log["record"],
                                         log["user"],
                                         log["address"],
                                         log["hostname"],
                                         log["point"],
                                         log["domain"],
                                         server_id))

                self.cursor.execute('SELECT record FROM log_auth WHERE server_id = %s ORDER BY record DESC LIMIT 1 ', (server_id,))
                last_record = self.cursor.fetchone()[0]
            finally:
                for log in args:
                    if log['record'] > last_record:
                        self.cursor.execute("INSERT INTO log_auth("
                                            "time, "
                                            "event_id, "
                                            "record, "
                                            "username, "
                                            "address, "
                                            "hostname, "
                                            "point, "
                                            "domain, "
                                            "server_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                            (log["time"],
                                             log["event_id"],
                                             log["record"],
                                             log["user"],
                                             log["address"],
                                             log["hostname"],
                                             log["point"],
                                             log["domain"],
                                             server_id))
                self.cursor.execute('SELECT record FROM log_auth WHERE server_id = %s ORDER BY record DESC LIMIT 1 ', (server_id,))
                last_record = self.cursor.fetchone()[0]
                self.conn.commit()

                return last_record
        else:
            try:
                self.cursor.execute('SELECT record FROM log_auth WHERE server_id = %s ORDER BY record DESC LIMIT 1 ', (server_id,))
                last_record = self.cursor.fetchone()[0]
            except:
                last_record = 0
            return last_record

    def insert_smart(self, args, server_id):
        try:
            self.cursor.execute("SELECT id, server_id FROM smarts WHERE server_id=%s", (server_id,))
            smarts = self.cursor.fetchall()
            for smart in smarts:
                self.cursor.execute("DELETE FROM attributes WHERE device_id=%s", (smart[0],))
            self.cursor.execute("DELETE FROM smarts WHERE server_id=%s", (server_id,))
            for smart in args:
                self.cursor.execute("INSERT INTO smarts("
                                    "model_name, "
                                    "serial_number, "
                                    "status, "
                                    "server_id, "
                                    "date) VALUES (%s,%s,%s,%s,%s) RETURNING id",
                                    (smart["model_name"],
                                     smart["serial_number"],
                                     smart["smart_status"]["passed"],
                                     server_id,
                                     time.ctime()))
                device_id = self.cursor.fetchone()[0]

                # attributes

                for attribute in smart["ata_smart_attributes"]['table']:
                    self.cursor.execute("INSERT INTO attributes("
                                        "id, "
                                        "name, "
                                        "value, "
                                        "worst, "
                                        "treshold, "
                                        "raw_value, "
                                        "device_id) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                                        (attribute["id"],
                                         attribute["name"],
                                         attribute["value"],
                                         attribute["worst"],
                                         attribute["thresh"],
                                         attribute["raw"]["string"],
                                         device_id))
            self.conn.commit()
        except Exception:
            pass

    def insert_uptime(self, args, server_id):

        try:
            self.cursor.execute("DELETE FROM uptime WHERE server_id=%s", (server_id,))
            self.cursor.execute("INSERT INTO uptime("
                                "server_id,"
                                "days, "
                                "hours, "
                                "minutes, "
                                "secounds) VALUES (%s,%s,%s,%s,%s)",
                                (server_id,
                                 args[0]['days'],
                                 args[0]['hours'],
                                 args[0]['minutes'],
                                 args[0]['secounds']))
            self.conn.commit()
        except Exception:
            pass

    def insert_hwm(self, args, server_id):
        if "temperature" in args:
            try:
                self.cursor.execute("INSERT INTO sensors("
                                    "server_id, "
                                    "name, "
                                    "date) VALUES (%s,%s,%s) RETURNING id",
                                    (server_id,
                                     "temperature",
                                     time.ctime()))
                sensor_id = self.cursor.fetchone()[0]
                for items in args['temperature']:
                    for key, val in items.items():
                        self.cursor.execute("INSERT INTO sensor(sensor_id, key, value) VALUES (%s,%s,%s)",
                                            (sensor_id, key, int(val)))
                self.conn.commit()
            except Exception:
                pass

        if "load" in args:
            try:
                self.cursor.execute("INSERT INTO sensors("
                                    "server_id, "
                                    "name, "
                                    "date) VALUES (%s,%s,%s) RETURNING id",
                                    (server_id,
                                     "load",
                                     time.ctime()))
                sensor_id = self.cursor.fetchone()[0]
                for items in args['load']:
                    for key, val in items.items():
                        self.cursor.execute("INSERT INTO sensor(sensor_id, key, value) VALUES (%s,%s,%s)",
                                            (sensor_id, key, int(val)))
                self.conn.commit()
            except Exception:
                pass

        if "voltages" in args:
            try:
                self.cursor.execute("INSERT INTO sensors("
                                    "server_id, "
                                    "name, "
                                    "date) VALUES (%s,%s,%s) RETURNING id",
                                    (server_id,
                                     "voltages",
                                     time.ctime()))
                sensor_id = self.cursor.fetchone()[0]
                for items in args['voltages']:

                    for key, val in items.items():
                        self.cursor.execute("INSERT INTO sensor(sensor_id, key, value) VALUES (%s,%s,%s)",
                                            (sensor_id, key, val))
                self.conn.commit()
            except Exception:
                pass

        if "hardware" in args:
            try:
                self.cursor.execute("INSERT INTO sensors("
                                    "server_id, "
                                    "name, "
                                    "date) VALUES (%s,%s,%s) RETURNING id",
                                    (server_id,
                                     "hardware",
                                     time.ctime()))
                sensor_id = self.cursor.fetchone()[0]
                for items in args['hardware']:

                    for key, val in items.items():
                        self.cursor.execute("INSERT INTO sensor(sensor_id, key, value) VALUES (%s,%s,%s)",
                                            (sensor_id, key, val))
                self.conn.commit()
            except Exception:
                pass

    def insert_disk_space(self, args, server_id):
        try:
            for part in args:
                self.cursor.execute("INSERT INTO disk_space("
                                    "server_id, "
                                    "part, "
                                    "total, "
                                    "used, "
                                    "free, "
                                    "date) VALUES (%s,%s,%s,%s,%s,%s)",
                                    (server_id,
                                     part['part'],
                                     part['total'],
                                     part['used'],
                                     part['free'],
                                     time.ctime()))
            self.conn.commit()
        except Exception as e:
            print('insert_disk_space:' + e)

    def insert_services(self, args, server_id):
        try:
            self.cursor.execute("DELETE FROM services WHERE server_id=%s", (server_id,))
            for service in args:
                self.cursor.execute("INSERT INTO services("
                                    "display_name, "
                                    "binpath, "
                                    "username, "
                                    "start_type, "
                                    "status, "
                                    "pid, "
                                    "name, "
                                    "date, "
                                    "server_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                    (service['display_name'],
                                     service['binpath'],
                                     service['username'],
                                     service['start_type'],
                                     service['status'],
                                     service['pid'],
                                     service['name'],
                                     time.ctime(),
                                     server_id))
            self.conn.commit()
        except Exception:
            pass

    def insert_data(self, args):
        server_id = self.get_server_id(args['name_id'])
        response = dict()
        if 'uptime' in args:
            self.insert_uptime(args['uptime'], server_id)
        if 'log_auth' in args:
            response['last_record'] = self.insert_log_auth(args['log_auth'], server_id)
        if 'smart' in args:
            self.insert_smart(args['smart'], server_id)
        if 'hwm' in args:
            self.insert_hwm(args['hwm'], server_id)
        if 'disk_space' in args:
            self.insert_disk_space(args['disk_space'], server_id)
        if 'services' in args:
            self.insert_services(args['services'], server_id)
        return response
