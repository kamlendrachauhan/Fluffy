# Fluffy

Fluffy is a distributed file storage system built for storing and retrieving the files. Users are allowed to store and retrieve any type of file varying from docx, pdf, zip documents also images of format PNG, JPEG and multimedia file like mp3. It’s a highly scalable, secure, fault tolerant and efficient file storage system able to handle multiple queries simultaneously.

## Instructions to run

### Prerequisites : 
    This project requires the following JAR files(They are present in the /lib folder of the project).
    
    jackson-all-1.8.5.jar
    netty-all-4.0.15.Final.jar
    slf4j-api-1.7.2.jar
    slf4j-simple-1.7.2.jar
    mongo-java-driver-3.2.2.jar
    protobuf-java-2.6.1.jar
    json-20160212.jar
    riak-client-2.0.5.jar

In the terminal, navigate to the folder directory where the project files are located.
Run the shell script build_pb.sh to build the .proto files as follows : ./build_pb.sh
Run the ant build script build.xml to build the project. 
Run the shell script startServer.sh and provide the following arguments to run it. 
<routing-conf-filename> <global-routing-conf-filename> <isMonitorEnabled>

<routing-conf-filename> - The file which contains the routing information for the node. 
Eg : route-1.conf.

<global-routing-conf-filename> -  The file which contains the information for the Global Configuration of the node.
Eg : route-globalconf-4.conf

<isMonitorEnabled> - a true/false flag which states that the monitor is enabled. Usually set to false.

Run the shell script as follows  :
./startServer.sh  route-1.conf route-1.conf false.
The server is now started.
For the java based client, run the gash.router.app.DemoApp.java file and follow the instructions on screen.
For the python based client, run the BasicClientApp.py as follows : 

python BasicClientApp.py

And follow the on-screen instructions to test the network.

## Future Work

    1. The Outbound queues are currently processing the enqueued messages synchronously, even though most of the time, the outbound messages need to be written to different channels. The outbound queues could be modified in the sense that the QueueManager could hold a list of outbound queues in a dictionary/map structure and retrieve it from there, to be written. This could increase throughput for file read/writes as well as client communication. 
    2. As of now replication is propagating to all the nodes on the network. Although this provides resiliency, it is inefficient as data is redundantly being stored on all the nodes. We intend to replicate data on a few of the nodes using some processing logic.
    3. The work port of every node is overloaded with different types of messages(node discovery, leader election, replication and read requests and responses). We intend to create a separate port for at least the replication and read messages, so that they don’t conflict during channel read/writes with the other types of messages.
    4. Introduction of log replication and consensus in the RAFT architecture .
    5. When the client sends a WRITE request but the space available in current cluster is not sufficient then the request should be forwarded to another cluster. Leader converts the CommandMessage to GlobalCommandMessage and forwards it to its own adapter A1. A1 forwards it to one of the adapters. The adapter checks the same and does the intended operation.
    6. Introduction of redirection logic so that the client is redirected to the leader node in the network, so that it does not have to know the leader beforehand.
    7. Introduction of direct READ from the follower node so that the leader need not handle the delegation response, and the data is passed directly from the follower node to the client, instead of through the leader.
    8. Introduction of redirection from the client to the node that can service its READ request.

