#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Single script module that provide classes of roles for atomic broadcasting using Paxos.
Running this script with parameters path_to_config, role_name and id will start an instance of that role.
path_to_config: Path to your config file. The config file defines the multicast ip for different kinds of roles.
role_name: The type of role you want to run.
id: The id of this role instance.

Class
Role: Basic Role class, defining common methods, properties and utility functions.
Acceptor: Acceptor role class, defining the behavior and properties of role acceptor in our protocol.
Proposer: Proposer role class, defining the behavior and properties of role proposer in our protocol.
Learner: Learner role class, defining the behavior and properties of role learner in our protocol.
Client: Client role class, defining role client for producing data.
"""


import sys
import socket
import struct
import logging
import time
import threading
import signal


class Role(object):
    """Basic Role class.

    Attributes
        role: the name of the role.
        host_port: the multicast ip for this kind of role.
        receive_socket: socket.socket object that receives udp message from this role's multicast ip
        send_socket: socket.socker object that is used for send udp message to a given ip.

    Public Methods
    __init__
    run: Set the sockets for using and register handle function for signal SIGTERM.

    Static Methods
    parse_config: read the config file and return a dictionary
                    whose entries are the names of roles and values are their multicast ips.
    signal_term_handle: kill the main thread if receives signal.SIGTERM.
    """

    def __init__(self, role_name, host_port):
        """Set basic properties."""

        super().__init__()
        self.role = role_name
        self.host_port = host_port
        self.receive_socket = None
        self.send_socket = None

        logging.info("{} host port is {}".format(role_name, host_port))

    def _set_multicast_receiver(self):
        """Set self.receive_socket listen to its multicast ip"""

        receive_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        receive_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        receive_socket.bind(self.host_port)

        multicast_group = struct.pack("4sl", socket.inet_aton(self.host_port[0]), socket.INADDR_ANY)
        receive_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, multicast_group)
        self.receive_socket = receive_socket
    
    def _set_multicast_sender(self):
        """Set socket for send messages"""

        send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.send_socket = send_socket
    
    def run(self):
        """Do some contextual work. Should add specific behavior for different subclasses."""

        self._set_multicast_receiver()
        self._set_multicast_sender()

        signal.signal(signal.SIGTERM, Role.signal_term_handle)

    @staticmethod
    def parse_config(path_to_config):
        """read the config file and return a dictionary
        whose entries are the names of roles and values are their multicast ips.
        """

        config = {}
        with open(path_to_config, 'r') as config_file:
            for line in config_file:
                (role, host, port) = line.split()
                config[role] = (host, int(port))
        return config

    @staticmethod
    def signal_term_handle(_signal, frame):
        """Kill main thread if receives signal.SIGTERM"""

        logging.info("Killed by signal term...")
        sys.exit(0)


class Acceptor(Role):
    """Class for acceptors. Acceptor will forward the messages decided by the leader of proposers
    to learners in the order they are decided.

    Attribute
        id: id of the instance
        config: dictionary that keep the multicast ips for different roles.
        message_received: set of (seq,value) that received.
        message_forward: set of (seq,value) that already be forwarded to learners.
        forward_count: The number of values that is forwarded. Only forward a message (seq,value) if its seq equals
            forward_count to ensure that the messages are forward in total order.

    Public method
    run: Defining the behavior of acceptors. The acceptor will start two threads.
            One distributes different messages that it receives to different handle functions base on their tags.
            Another forwards the messages that decided by the leader of proposers in the order they are decided.
    """
    def __init__(self, config, id):
        host_port = config['acceptors']
        super().__init__('Acceptor', host_port)
        self.id = id
        self.config = config

        self.message_received = set()
        self.message_forward = set()
        self.forward_count = 0
    
    def run(self):
        """Keep the messages decided by leader of proposers and forward them to learner in the order they are decided"""

        super().run()
        print("{} {} starts running...".format(self.role, self.id))
        
        threads = []

        thread_distribute_message = threading.Thread(target=self.distribute_message_received, daemon=True)
        thread_forward_message = threading.Thread(target=self.forward_to_learner, daemon=True)

        threads.append(thread_forward_message)
        threads.append(thread_distribute_message)

        for thread in threads:
            thread.start()

        while True:
            time.sleep(5)
            logging.info("Main Thread running...")
    
    def distribute_message_received(self):
        """Distribute messages to different handle functions based on their tags.

        Message type
        "decide:seq,value": The message representing a value is decided at time seq.
        "catchupReq:seq": request sent by learner for asking messages that are later than seq.
        """

        handle_dict = {
            "decide": self.handle_decide,
            "catchupReq":self.send_catchup_response,
        }

        while True:
            logging.info("{} {} handle receiving...".format(self.role, self.id))

            raw_message = self.receive_socket.recv(2**16)
            raw_message = raw_message.decode("utf-8")

            # get tag and message from the raw message
            tag, message = raw_message.split(":")
            
            handle_dict[tag](message)

    def handle_decide(self, message):
        """Handle the message of type "decide:seq,value". seq is the index of value in the total order.
        Keep (seq,value) if not receive it before.
        """

        message = tuple(message.split(","))
        logging.info("{} {} received decide:{},{}".format(self.role, self.id, *message))
        if not message in self.message_received:
            self.message_received.add(message)

    def send_catchup_response(self, message):
        """If this acceptor has the missed messages, send a response contain these messages."""

        request_sequence = int(message)
        logging.info("{} {} received catchup request of sequence {}".format(self.role, self.id, request_sequence))
        response_list = []
        for message in self.message_received:
            sequence, value = message
            if int(sequence) >= request_sequence:
                response_str = "{}.{}".format(sequence, value)
                response_list.append(response_str)
        if len(response_list) > 0:
            raw_message = ";".join(response_list)
            raw_message = "catchupRes:{},{}".format(request_sequence, raw_message)
            logging.info("{} {} send catchup response {} to learners".format(self.role, self.id, raw_message))
            raw_message = bytes(raw_message, "utf-8")

            self.send_socket.sendto(raw_message, self.config["learners"])

    def forward_to_learner(self):
        """Forward the messages received but not forwarded in their decided order."""

        while True:
            messages_not_forward = self.message_received - self.message_forward
            if bool(messages_not_forward):
                for decide_sequence, value in list(messages_not_forward):
                    if int(decide_sequence) == self.forward_count:
                        message = (decide_sequence, value)
                        message = "forward:{},{}".format(*message)
                        logging.info("{} {} is sending {} to learners".format(self.role, self.id, message))
                        message = bytes(message, "utf-8")
                        self.send_socket.sendto(message, self.config["learners"])
                        self.forward_count += 1


class Proposer(Role):
    """Class for proposers. Proposer will forward the value they receive for the first time to the leader.
    Leader will decide values in order and send them to learners.

    Attributes
        id: id of the instance
        config: dictionary that keep the multicast ips for different roles.
        message_decided: set of (seq,value) that decided.
        message_proposed: set of (client_id,value) that produced by client.
        decide_count: The number of values decided by leader. Composing it with the value decided to specify the time
            that value is decided.

    Public method
    run: Defining the behavior of proposer. The proposer will start three threads.
            One sends heart beat to other proposers for selecting the leader.
            Another distributes messages based on their tags.
            The last decides a value and forwards the decision each time if this proposer is a leader.
    """
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
        """Select leader, make decision and handle messages received"""

        super().run()
        logging.info("{} {} starts running...".format(self.role, self.id))

        threads = []

        thread_living_heartbeat = threading.Thread(target=self.living_heartbeat, daemon=True)
        thread_distribute_message = threading.Thread(target=self.distribute_message_received, daemon=True)
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
        """Send heart beat to all proposers every 1 second"""

        while True:
            message = "living:{}".format(self.id)
            message = bytes(message, "utf-8")
            self.send_socket.sendto(message, self.config["proposers"])
            # logging.info("{} {} send heartbeat...".format(self.role, self.id))

            time.sleep(1)

    def distribute_message_received(self):
        """Distribute messages to different handle functions based on their tags.

        Message type
            "living:id": heart beat message for selecting leader.
            "propose:(client_id,value)": value produced by different client.
            "Pdecide:(seq,value)": decision made by leader.
        """
        handle_dict = {
            "living": self.handle_leader_consensus,
            "propose": self.handle_value_propose,
            "Pdecide": self.handle_value_decide,         
        }

        while True:

            # logging.info("{} {} handle recieving...".format(self.role, self.id))

            raw_message = self.receive_socket.recv(2**16)
            raw_message = raw_message.decode("utf-8")
            tag, message = raw_message.split(":")

            handle_dict[tag](message)

    def decide_a_value(self):
        """If this proposer is leader, decide a value each step if there is value undecided"""

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
        """Select leader based on the heart beat. Keep a set of living proposers, and select the proposer with min id as
        leader. A proposer is considered living if the last time that this proposer receives its heart beat is no later
        than three seconds ago.
        """

        id = int(message)
        # logging.info("{} {} received living proposer {}".format(self.role, self.id, id))
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
        """Keep and forward the propose to other proposers if receive it for the first time."""

        message = tuple(message.split(","))
        logging.info("{} {} received propose:{},{}".format(self.role, self.id, *message))
        
        if not message in self.message_proposed:
            self.message_proposed.add(message)
            raw_message = "propose:{},{}".format(*message)
            logging.info("{} {} is sending {} to proposers".format(self.role, self.id, raw_message))
            raw_message = bytes(raw_message, "utf-8")
            self.send_socket.sendto(raw_message, self.config["proposers"])

    def handle_value_decide(self, message):
        """Decide a value with seq if there are values not decided and this proposer is a leader"""

        message = tuple(message.split(','))
        logging.info("{} {} received Pdecide:{},{}".format(self.role, self.id, *message))

        leader_id = min(self.living_proposers_id)
        if self.id != leader_id:
            self.decide_count += 1
            if not message in self.message_proposed:
                self.message_proposed.add(message)   
            if not message in self.message_decided:
                self.message_decided.add(message)


class Learner(Role):
    """Class for learners. The learner will write value decided in their decided order.

    Attributes
        id: id of instance
        config: dictionary that keep the multicast ips for different roles.
        message_received: messages that learner has received.
        count: the number of value that is written. Only write a value if its corresponding seq equals to count
                to ensure that learner writes in total order.

    Public method
    run: Writing value in total order. Ask other learners for messages if it finds itself missing some messages.
    """

    def __init__(self, config, id):
        host_port = config['learners']
        super().__init__('Learner', host_port)
        self.id = id
        self.config = config
        
        self.message_received = []
        self.count = 0

    def run(self):
        """Write value in total order. Ask other learners for messages if it finds itself missing some messages."""

        super().run()
        logging.info("{} {} starts running...".format(self.role, self.id))

        threads = []

        thread_distribute_message = threading.Thread(target=self.distribute_message_received, daemon=True)
        thread_write_value = threading.Thread(target=self.write, daemon=True)

        threads.append(thread_write_value)
        threads.append(thread_distribute_message)

        for thread in threads:
            thread.start()

        while True:
            time.sleep(5)
            logging.info("Main Thread running...")

    def distribute_message_received(self):
        """Distribute messages to different handle functions based on their tags.

        Message type
            "forward:seq,value": message forwarded by acceptor.
            "catchupRes:seq,seq.value;[seq.value;]+" response contain missed messages.
        """

        handle_dict = {
            "forward": self.handle_forward,
            "catchupRes": self.handle_catchup_response,
        }
        
        while True:
            logging.info("{} {} handle receiving...".format(self.role, self.id))

            raw_message = self.receive_socket.recv(2**16)
            raw_message = raw_message.decode("utf-8")
            tag, message = raw_message.split(":")

            handle_dict[tag](message)

    def handle_forward(self, message):
        """If the message is not received before, keep it. If the seq of the message received is great than self.count,
        then ask acceptors for the missed messages.
        """

        message = tuple(message.split(","))
        logging.info("{} {} received forward:{},{}".format(self.role, self.id, *message))

        sequence, value = message

        if int(sequence) >= self.count:
            if not message in self.message_received:
                self.message_received.append(message)
            if int(sequence) > self.count:
                raw_message = "catchupReq:{}".format(self.count)
                logging.info("{} {} send catchup request {} to acceptors".format(self.role, self.id, raw_message))
                raw_message = bytes(raw_message, "utf-8")
                self.send_socket.sendto(raw_message, self.config["acceptors"])

    def handle_catchup_response(self, message):
        """If the request is sent by this learner, keep the messages in response."""

        request_sequence, response = tuple(message.split(","))
        if int(request_sequence) == self.count:
            logging.info("{} {} received catup response of sequence {}: {}".format(self.role, self.id, request_sequence, response))
            messages = [tuple(message.split(".")) for message in response.split(";")]
            for message in messages:
                if not message in self.message_received:
                    self.message_received.append(message)

    def write(self):
        """write the value in the order they are decided."""

        while True:
            if len(self.message_received) > 0:
                break
        while True:
            for sequence, message in self.message_received:
                if int(sequence) == self.count:
                    logging.info("{} {} write {}".format(self.role, self.id, message))
                    print(message)
                    sys.stdout.flush()
                    self.count += 1 


class Client(Role):
    """Class for client. client only proposes the value from system input to proposers

    Attribute
        id: id of instance
        config: dictionary that keep the multicast ips for different roles.

    Public method
    run: propose value from system input.
    """

    def __init__(self, config, id):
        host_port = config['clients']
        super().__init__('Client', host_port)
        self.id = id
        self.config = config
    
    def run(self):
        """propose value from system input."""

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
    """Initialize new role with parameters given."""

    # set level = logging.INFO if you want to see detailed log.
    logging.basicConfig(level=logging.WARNING)

    # parse path of config file, type of role and it's id.
    path_to_config = sys.argv[1]
    role = sys.argv[2]
    role_id = int(sys.argv[3])

    config = Role.parse_config(path_to_config)

    # Dictionary that map the role:string to its corresponding class, so that different kinds of roles
    # could be initialized with the same code.
    role_dict = {
        "proposer": Proposer,
        "acceptor": Acceptor,
        "learner": Learner,
        "client": Client,
    }

    # Initialize the expected role object.
    role_instance = role_dict[role](config, role_id)

    # start running
    role_instance.run()
