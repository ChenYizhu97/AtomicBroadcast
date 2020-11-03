#!/usr/bin/env python3
import sys
import socket
import struct
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import threading
import signal

class Role(object):
    def __init__(self, role_name, host_port):
        super().__init__()
        self.role= role_name
        self.host_port = host_port
        logging.info("{} hostport is {}".format(role_name, host_port))

    def set_multicast_reciever(self):
        recieve_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        recieve_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        recieve_socket.bind(self.host_port)

        multicast_group = struct.pack("4sl", socket.inet_aton(self.host_port[0]), socket.INADDR_ANY)
        recieve_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, multicast_group)
        self.recieve_socket = recieve_socket
    
    def set_multicast_sender(self):
        send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.send_socket = send_socket
    
    def run(self):
        self.set_multicast_reciever()
        self.set_multicast_sender()

        signal.signal(signal.SIGTERM, Role.signal_term_handle)

    @staticmethod
    def parse_config(path_to_config):
        config = {}
        with open(path_to_config, 'r') as config_file:
            for line in config_file:
                (role, host, port) = line.split()
                config[role] = (host, int(port))
        return config

    @staticmethod
    def signal_term_handle(signal, frame):
        logging.info("Killed by signal term...")
        sys.exit(0)

class Acceptor(Role):
    def __init__(self, config, id):
        host_port = config['acceptors']
        super().__init__('Acceptor', host_port)
        self.id = id
        self.config = config

        self.message_recieved = set()
        self.message_forward = set()
        self.forward_count = 0
    
    def run(self):
        
        super().run()
        print("{} {} starts running...".format(self.role, self.id))
        
        threads = []

        thread_distribute_message = threading.Thread(target=self.distribute_message_recieved, daemon=True)
        thread_forward_message = threading.Thread(target=self.forward_to_learner, daemon=True)

        threads.append(thread_forward_message)
        threads.append(thread_distribute_message)

        for thread in threads:
            thread.start()

        while True:
            time.sleep(5)
            logging.info("Main Thread running...")
    
    def distribute_message_recieved(self):
        handle_dict = {
            "decide": self.handle_decide,
        }
        while True:

            logging.info("{} {} handle recieving...".format(self.role, self.id))

            raw_message = self.recieve_socket.recv(2**16)

            raw_message = raw_message.decode("utf-8")
            tag, message = raw_message.split(":")
            
            handle_dict[tag](message)

    def handle_decide(self, message):
        message = tuple(message.split(","))
        logging.info("{} {} recieved decide:{},{}".format(self.role, self.id, *message))
        if not message in self.message_recieved:
            self.message_recieved.add(message)

    def forward_to_learner(self):
        while True:
            messages_not_forward =  self.message_recieved - self.message_forward
            if bool(messages_not_forward):
                for decide_sequence, value in list(messages_not_forward):
                    if int(decide_sequence) == self.forward_count:
                        message = (decide_sequence, value)
                        message = "forward:{},{}".format(*message)
                        logging.info("{} {} is sending {} to leaners".format(self.role, self.id, message))
                        message = bytes(message, "utf-8")
                        self.send_socket.sendto(message, self.config["learners"])
                        self.forward_count += 1



class Proposer(Role):
    def __init__(self, config, id):
        host_port = config['proposers']
        super().__init__('Proposer', host_port)
        self.id = id
        self.config = config

        self.living_proposers_id = []
        self.living_proposers_timer = []

        self.message_decided = set()
        self.message_proposed = set()
        self.decide_count = 0

    def run(self):
        super().run()
        logging.info("{} {} starts running...".format(self.role, self.id))

        threads = []

        thread_living_heartbeat = threading.Thread(target=self.living_heartbeat, daemon=True)
        thread_distribute_message = threading.Thread(target=self.distribute_message_recieved, daemon=True)
        thread_decide_value = threading.Thread(target=self.decide_a_value, daemon=True)

        threads.append(thread_living_heartbeat)
        threads.append(thread_decide_value)
        threads.append(thread_distribute_message)

        for thread in threads:
            thread.start()

        while True:
            time.sleep(5)
            logging.info("Main Thread running...")

    def living_heartbeat(self):
        while True:
            message = "living:{}".format(self.id)
            message = bytes(message, "utf-8")
            self.send_socket.sendto(message, self.config["proposers"])
            # logging.info("{} {} send heartbeat...".format(self.role, self.id))

            time.sleep(1)

    def distribute_message_recieved(self):
        handle_dict = {
            "living": self.handle_leader_consensus,
            "propose": self.handle_value_propose,
            "Pdecide": self.handle_value_decide,         
        }

        while True:

            # logging.info("{} {} handle recieving...".format(self.role, self.id))

            raw_message = self.recieve_socket.recv(2**16)
            raw_message = raw_message.decode("utf-8")
            tag, message = raw_message.split(":")

            handle_dict[tag](message)

    def decide_a_value(self):
        while True:
            if len(self.living_proposers_id) > 0:
                break
        while True:
            leader_id = min(self.living_proposers_id)
            
            if self.id != leader_id:
                continue
            
            messages_not_decided = self.message_proposed - self.message_decided
            if bool(messages_not_decided) :
                message_to_decide = messages_not_decided.pop()

                _, value = message_to_decide
                decide_message_to_acceptor = (self.decide_count, value)

                raw_message = "decide:{},{}".format(*decide_message_to_acceptor)
                logging.info("{} is sending {} to acceptors".format(self.role, raw_message))
                raw_message = bytes(raw_message, "utf-8")
                self.send_socket.sendto(raw_message, self.config["acceptors"])

                raw_message = "Pdecide:{},{}".format(*message_to_decide)
                logging.info("{} is sending {} to proposers".format(self.role, raw_message))
                raw_message = bytes(raw_message, "utf-8")
                self.send_socket.sendto(raw_message, self.config["proposers"])
                
                self.decide_count += 1
                self.message_decided.add(message_to_decide)

    def handle_leader_consensus(self, message):
        id = int(message)
        # logging.info("{} {} recieved living proposer {}".format(self.role, self.id, id))
        time_now = time.time()
        if id not in self.living_proposers_id:
            self.living_proposers_id.append(id)
            self.living_proposers_timer.append(time_now)
        else:
            self.living_proposers_timer[self.living_proposers_id.index(id)] = time_now

        index_of_dead_proposers = []
        for index, time_last_seen in enumerate(self.living_proposers_timer):
            if time_now - time_last_seen > 3:
                index_of_dead_proposers.append(index)

        self.living_proposers_id = [id for index, id in enumerate(self.living_proposers_id) if not index in index_of_dead_proposers]
        self.living_proposers_timer = [time_last_seen for index, time_last_seen in enumerate(self.living_proposers_timer) if not index in index_of_dead_proposers]

    def handle_value_propose(self, message):
        message = tuple(message.split(","))
        logging.info("{} {} recieved propose:{},{}".format(self.role, self.id, *message))
        
        if not message in self.message_proposed:
            self.message_proposed.add(message)
            raw_message = "propose:{},{}".format(*message)
            logging.info("{} {} is sending {} to proposers".format(self.role, self.id, raw_message))
            raw_message = bytes(raw_message, "utf-8")
            self.send_socket.sendto(raw_message, self.config["proposers"])

    def handle_value_decide(self, message):
        message = tuple(message.split(','))
        logging.info("{} {} recieved Pdecide:{},{}".format(self.role, self.id, *message))

        leader_id = min(self.living_proposers_id)
        if self.id != leader_id:
            self.decide_count += 1
            if not message in self.message_proposed:
                self.message_proposed.add(message)   
            if not message in self.message_decided:
                self.message_decided.add(message)


class Learner(Role):
    def __init__(self, config, id):
        host_port = config['learners']
        super().__init__('Learner', host_port)
        self.id = id
        self.config = config
        
        self.message_recieved = []
        self.count = 0

    def run(self):
        """Run threads of the learner"""
        super().run()
        logging.info("{} {} starts running...".format(self.role, self.id))

        threads = []

        thread_distribute_message = threading.Thread(target=self.distribute_message_recieved, daemon=True)
        thread_write_value = threading.Thread(target=self.write, daemon=True)

        threads.append(thread_write_value)
        threads.append(thread_distribute_message)

        for thread in threads:
            thread.start()

        while True:
            time.sleep(5)
            logging.info("Main Thread running...")

    def distribute_message_recieved(self):

        handle_dict = {
            "forward": self.handle_forward,
            "catchupReq": self.send_catchup_response,
            "catchupRes": self.handle_catchup_response,
        }
        
        while True:
            logging.info("{} {} handle recieving...".format(self.role, self.id))

            raw_message = self.recieve_socket.recv(2**16)
            raw_message = raw_message.decode("utf-8")
            tag, message = raw_message.split(":")

            handle_dict[tag](message)

    def handle_forward(self, message):
        message = tuple(message.split(","))
        logging.info("{} {} recieved forward:{},{}".format(self.role, self.id, *message))

        sequence, value = message

        if int(sequence) >= self.count:
            if not message in self.message_recieved:
                self.message_recieved.append(message)
            if int(sequence) > self.count:
                raw_message = "catchupReq:{}".format(self.count)
                logging.info("{} {} send catchup request {} to learners".format(self.role, self.id, raw_message))
                raw_message = bytes(raw_message, "utf-8")
                self.send_socket.sendto(raw_message, self.config["learners"])

    def send_catchup_response(self, message):
        request_sequence = int(message)
        logging.info("{} {} received catchup request of sequence {}".format(self.role, self.id, request_sequence))
        response_list = []
        for message in self.message_recieved:
            sequence, value = message
            if int(sequence) >= request_sequence:
                response_str="{}.{}".format(sequence,value)
                response_list.append(response_str)
        raw_message = ";".join(response_list)
        raw_message = "catchupRes:{},{}".format(request_sequence, raw_message) 
        logging.info("{} {} send catchup response {} to learners".format(self.role, self.id, raw_message))
        raw_message = bytes(raw_message, "utf-8")

        self.send_socket.sendto(raw_message, self.config["learners"])

    def handle_catchup_response(self, message):
        request_sequence, response = tuple(message.split(","))
        if int(request_sequence) == self.count:
            logging.info("{} {} recieved catup response of sequence {}: {}".format(self.role, self.id, request_sequence, response))
            messages = [tuple(message.split(".")) for message in response.split(";")]
            for message in messages:
                if not message in self.message_recieved:
                    self.message_recieved.append(message)


    def write(self):
        while True:
            if len(self.message_recieved) > 0:
                break
        while True:
            for sequence, message in self.message_recieved:
                if int(sequence) == self.count:
                    logging.info("{} {} write {}".format(self.role, self.id, message))
                    print(message)
                    sys.stdout.flush()
                    self.count += 1 


class Client(Role):
    def __init__(self, config, id):
        host_port = config['clients']
        super().__init__('Client', host_port)
        self.id = id
        self.config = config
    
    def run(self):
        super().run()
        logging.info("{} {} starts running...".format(self.role, self.id))
        for value in sys.stdin:
            value = value.strip()
            message = "propose:{},{}".format(self.id, value)
            message = bytes(message, "utf-8")
            logging.info("{} {} is sending {} to proposers".format(self.role, self.id, value))
            self.send_socket.sendto(message, self.config["proposers"])
        print("{} {} is done.".format(self.role, self.id))


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    path_to_config = sys.argv[1]
    role = sys.argv[2]
    id = int(sys.argv[3])

    config = Role.parse_config(path_to_config)
    role_dict = {
        "proposer": Proposer,
        "acceptor": Acceptor,
        "learner": Learner,
        "client": Client,
    }

    role_instance = role_dict[role](config, id)
    role_instance.run()