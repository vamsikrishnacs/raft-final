raft-final-
===========

raft leader election algorithm
-----------------------

About:
---------
Implemeted Raft leader election algorithm in go language.It is essentially a group of servers communicating via RPC's to elect a leader using the leader election algorithm in RAFT.

Each Replicator object contains a kvstore of its own which presently is customised to test on Student Details database(kvstore)


What it does:
------------------------
It is set of servers which have a dedicated kvstore of their own,they communicate using the Raft consensus algorithm.You can basically use it to create a kvstore on a server that replicates itself to mutiple servers,you can use it to send requests to any random server in the group to get the response

It does the job of performing the replication and taking care of server crashes and database consistency

Architecture
-------------------------

There is a kv store client that communicates to kvstore server

The kvstore server puts the object on its inbox and thereby replicating it on all the peers,once succesfully replicated the message response is sent back

The user of the kvstore(client) can send the request to any one of these pool of servers and get a consistent information irregardless of server crashes


Features:
-----------
1. The package raft is a replicator object capable of communicating with other raft objects in order to establish a leader.
2. A New object can be created using

raft.New(1,config file path)

3. The replicators communicate using the zeromq message passing mechanism and conisits of inbox() and outbox().In order to send a message

s.outbox()<-&raft.Envelope{pid,Message}

4. The internal communication is automatically handled within the raft object itself and it is capable of finding the leader or establish itself as a leader.

5.Provide a configuration file which contains peer adresses,election-timeouts,default heartbeat-interval


Components
-------------------
1. raft.go (consensus)
2. kvstore (state machine of the server)


Usage:
----------------
1. go get github.com/vamsikrishnacs/raft-final
2. go test

Output:
---------------
1. A logitems1.txt file will be there that shows the details of the transactions including the get,put and delete.
2. part form that logs of all the servers are created where you can see the that all the logs are consistent with the order of the operations.

Testing(System)
-------------------

1. Raft is invoked from the raft_test.go client program
2. It initilaises the raft set of servers in action and waiting for the objects on their respective interfaces
3. A Kvstore(server) program consists of a datbase of students(Rollno,address,phone)
4. The kvclient program initialises mutiple clients which sends requests to all the kvstores in parallel in random.
5. The kvstore server after necessary processing i.e., succesful replication makes sure that the whole set of transactions are consistent
6. Test:The consistency is checked by creating a random sequence of operations and after some time,after a set of kill and restart operations the results are queried
7. The results indicate the consistency and validity of the database 



Testing(raft Usage)
-------------------
1. Used a timeout to kill the leader every 7 seconds,after a leader is killed candidates emerge and after necessary processing(election) a new leader emerge.
2. After killing half of the processes no update to file is made i.e..,system comes to a halt
3. while testing a (tick) in command line is seen to ensure the Heartbeat(a leader) is live.
4. At halt state (tick) stops








