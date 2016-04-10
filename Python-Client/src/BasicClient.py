import socket
import common_pb2
import pipe_pb2
import pbd
from protobuf_to_dict import protobuf_to_dict
from asyncore import read
import struct
import time

class BasicClient:
    def __init__(self,host,port):
        self.host = host
        self.port = port
        self.sd  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def setName(self,name):
        self.name = name

    def getName(self):
        return self.name
    
    def startSession(self):
        self.sd.connect((self.host,self.port))
        print("Connected to Host:",self.host,"@ Port:",self.port)

    def stopSession(self):
        builder = MessageBuilder()
        msg = builder.encode(MessageType.leave, self.name,'', '')
        print(msg)
        self.sd.send(msg)
        self.sd.close()
        self.sd = None
    
    def join(self,name):
        builder = MessageBuilder()
        self.name = name
        msg = builder.encode(MessageType.join, name, '', '')
        self.sd.send(msg)
        
    def sendMessage(self,message):
        if len(message) > 1024:
            print('message exceeds 1024 size')
            
        print('something')    
        builder = MessageBuilder()
        msg = builder.encode(MessageType.msg,self.name,message,'')
        self.sd.send(msg)
        
    def getFile(self,name):
        cm = pipe_pb2.CommandMessage();
        cm.header.node_id = 999;
        cm.header.time = 11234;
        cm.task.task_type = common_pb2.Task.TaskType.Value("READ")
        cm.task.sender = "127.0.0.1"
        cm.task.filename = name
        cm.message = name
        request = cm.SerializeToString()
        return request
    
    def chunkFile(self,file):
        fileChunk = []
        with open(file, "rb") as fileContent:
            data = fileContent.read(1024*1024)
            while data:
                fileChunk.append(data)
                data = fileContent.read(1024*1024) 
        return fileChunk
        
    def genPing(self):
        cm = pipe_pb2.CommandMessage();
        cm.header.node_id = 999;
        cm.header.time = 11234;
        cm.ping = True;
        msg = cm.SerializeToString();
        return msg;
    
    def genChunkedMsg(self,filename,filecontent, noofchunks,chunkid):

        cm = pipe_pb2.CommandMessage();
        cm.header.node_id = 999;
        cm.header.time = 11234;

        cm.task.task_type = common_pb2.Task.TaskType.Value("WRITE")
        cm.task.sender = "127.0.0.1"
        cm.task.filename = filename
        cm.task.chunk_no = chunkid
        cm.task.no_of_chunks = noofchunks
        cm.fileContent = filecontent;     
        msg = cm.SerializeToString();
        return msg
    
    def sendData(self,data,host,port):

        msg_len = struct.pack('>L', len(data))

        self.sd.sendall(msg_len + data)

        len_buf = self.receiveMsg(self.sd, 4)

        msg_in = self.receiveMsg(self.sd, len_buf)

        r =  pipe_pb2.CommandMessage();

        r.ParseFromString(msg_in)

        self.sd.close

        return r
        


    def receiveMsg(self,socket, waittime):

        socket.setblocking(0)

        finaldata = []

        data = ''

        start_time = time.time()

        while 1:

            if data and time.time()-start_time > waittime:

                break

            elif time.time()-start_time > waittime*2:

                break

        try:

            data = socket.recv(8192)

            if data:

                finaldata.append(data)

                begin = time.time()

            else:

                time.sleep(0.1)

        except:

            pass

        print(finaldata)

        return ''.join(finaldata)
        