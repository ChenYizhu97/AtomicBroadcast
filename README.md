# AtomicBroadcast
Implementation of atomic broadcast using Paxos

## Enviroment
- Test Operating system: macOS Catalina 10.15.3
- Python 3.8.6

## Start an instance of a role
> cd ATOMICBROADCAST
> ./paxos.py test/paxos.conf role_name id
or for specific role
> cd ATOMICBROADCAST
> ./acceptor.sh id test/paxos.conf
> ./proposer.sh id test/paxos.conf
> ...
You can replace test/paxo.con with the path of your config file.

## Use the test script in directory test
> cd ATOMICBROADCAST/test
> ./run_1acceptor.sh ../ 100
> ./any_test_script ../ number_of_values_each_client
> ...