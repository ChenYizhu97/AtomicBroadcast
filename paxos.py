#!/usr/bin/env python3
import sys
import socket
import struct
import logging
import time
from concurrent.futures import ThreadPoolExecutor

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

    @staticmethod
    def parse_config(path_to_config):
        config = {}
        with open(path_to_config, 'r') as config_file:
            for line in config_file:
                (role, host, port) = line.split()
                config[role] = (host, int(port))
        return config

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
        
        with ThreadPoolExecutor() as executor:
            executor.submit(self.forward_to_learner)
            executor.submit(self.distribute_message_recieved)
    
    def distribute_message_recieved(self):
        handle_dict = {
            "decide": self.handle_decide,
            "forward": self.handle_forward
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

    def handle_forward(self, message):
        message = tuple(message.split(","))
        logging.info("{} {} recieved forward:{},{}".format(self.role, self.id, *message))
        sequence, message = message
        if int(sequence) >= self.forward_count:
            self.forward_count = int(sequence) + 1
            logging.info("{} {} self.forward_count in thread distrubute message is {}".format(self.role, self.id, self.forward_count))
            self.message_forward.add(message)
            if not message in self.message_recieved:
                self.message_recieved.add(message)

    def forward_to_learner(self):
        while True:
            messages_not_forward =  self.message_recieved - self.message_forward
            for decide_sequence, value in list(messages_not_forward):
                if int(decide_sequence) == self.forward_count:
                    message = (decide_sequence, value)
                    message = "forward:{},{}".format(*message)
                    logging.info("{} {} is sending {} to acceptors and leaners".format(self.role, self.id, message))
                    message = bytes(message, "utf-8")
                    self.send_socket.sendto(message, self.config["acceptors"])
                    self.send_socket.sendto(message, self.config["learners"])
                    logging.info("{} {} self.forward_count in thread forward to learner is {}".format(self.role, self.id, self.forward_count))



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
        with ThreadPoolExecutor() as executor:
            executor.submit(self.living_heartbeat)
            executor.submit(self.distribute_message_recieved)

    def living_heartbeat(self):
        while True:
            message = "living:{}".format(self.id)
            message = bytes(message, "utf-8")
            self.send_socket.sendto(message, self.config["proposers"])
            #logging.info("{} {} send heartbeat...".format(self.role, self.id))

            time.sleep(1)

    def distribute_message_recieved(self):
        handle_dict = {
            "living": self.handle_leader_consensus,
            "propose": self.handle_value_propose,
            "Pforward": self.handle_value_forward,
            "Pdecide": self.handle_value_decide,         
        }

        while True:

            #logging.info("{} {} handle recieving...".format(self.role, self.id))

            raw_message = self.recieve_socket.recv(2**16)
            raw_message = raw_message.decode("utf-8")
            tag, message = raw_message.split(":")

            handle_dict[tag](message)
               
    def handle_leader_consensus(self, message):
        id = int(message)
        #logging.info("{} {} recieved living proposer {}".format(self.role, self.id, id))
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

        messages_not_decided = self.message_proposed - self.message_decided
        message_to_decide = messages_not_decided.pop()

        leader_id = min(self.living_proposers_id)          
        if self.id != leader_id:
            raw_message = "Pforward:{},{}".format(*message_to_decide)
            logging.info("{} is sending {} to proposers".format(self.role, raw_message))
            raw_message = bytes(raw_message, "utf-8") 
            self.send_socket.sendto(raw_message, self.config["proposers"])           
        else:
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
    
    def handle_value_forward(self, message):
        message = tuple(message.split(","))
        logging.info("{} {} recieved Pforward:{},{}".format(self.role, self.id, *message))
        leader_id = min(self.living_proposers_id)
        if self.id == leader_id:
            if not message in self.message_proposed:
                self.message_proposed.add(message)
                messages_not_decided = self.message_proposed - self.message_decided
                message_to_decide = messages_not_decided.pop()

                raw_message = "Pdecide:{},{}".format(*message_to_decide)
                logging.info("{} is sending {} to proposers".format(self.role, raw_message))
                raw_message = bytes(raw_message, "utf-8")
                self.send_socket.sendto(raw_message, self.config["proposers"])

                _, value = message_to_decide
                decide_message_to_acceptor = (self.decide_count, value)

                raw_message = "decide:{},{}".format(*decide_message_to_acceptor)
                logging.info("{} is sending {} to acceptors".format(self.role, raw_message))
                raw_message = bytes(raw_message, "utf-8")
                self.send_socket.sendto(raw_message, self.config["acceptors"])

                self.decide_count += 1
                self.message_decided.add(message_to_decid)
                



class Learner(Role):
    def __init__(self, config, id):
        host_port = config['learners']
        super().__init__('Learner', host_port)
        self.id = id
        self.config = config
        
        self.message_recieved = []
        self.count = 0

    def run(self):
        super().run()
        logging.info("{} {} starts running...".format(self.role, self.id))
        
        with ThreadPoolExecutor() as executor:
            executor.submit(self.write)
            executor.submit(self.distribute_message_recieved)
        

    def distribute_message_recieved(self):

        handle_dict = {
            "forward": self.handle_forward,
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
        if not message in self.message_recieved:
            self.message_recieved.append(message)
            self.message_recieved.sort(key=lambda message: int(message[0]))

    def write(self):
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
    logging.basicConfig(level=logging.NOTSET)
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