import sys
from BasicClient import BasicClient
    

class BasicClientApp:
    def run(self):
        name = None
        while name==None:
            print("Enter your name in order to join: ");
            name = sys.stdin.readline()
            if name==None:
                break
            print("Enter the IP of Server with which you want to connect: ");
            #host = raw_input();
            print("Enter the port of Server: ");
            #port = int(raw_input());
            
        port = 5101;
        host = "192.168.0.2"   
        bc =BasicClient(host,port)
        bc.startSession()
        bc.setName(name)
        #bc.join(name)

        print("\n--------****Fluffy Client****-------------" + name + "\n")
        print("Please Select one of following option by entering the respective number: \n");
        print("-----------------------------------------------\n");
        print("1.) Upload - Upload a New File \n"); 
        print("2.) Download - To Download a particular File\n");
        print("3.) help - list the options\n");
        print("4.) exit - end session\n");
        print("\n");

        forever = True;
        while (forever):
                #print("Enter your choice");
                #choice = int(raw_input());
                choice = 2;
                if (choice == None):
                    continue;
                elif (choice == 4 ):
                    print("Bye from Fluffy client!!");
                    bc.stopSession();
                    forever = False;
                elif (choice == 1):
                    print("Enter the name of your file: ")
                    #filename = raw_input();
                    print("SEND");
                    #bc.sendData(bc.genPing(),"127.0.0.1",4186);
                    filename = "intro.pdf"
                    print("Enter qualified pathname of file to be uploded: ");
                    #path = raw_input();
                    path = "introductions3.pdf";
                    chunks = bc.chunkFile(path)
                    noofchunks = len(chunks)

                    chunkid = 1;
                    for chunk in chunks:
                        req = bc.genChunkedMsg(filename,chunk,noofchunks,chunkid);
                        chunkid += 1 
                        result = bc.sendData(req,host,port)
                        
                    
                elif (choice == 2 ):
                    #print("Enter the filename you want to download: ")
                    #name = sys.stdin.readline()
                    name = "introductions3.pdf"
                    req = bc.getFile(name)
                    result = bc.sendData(req,host,port)
                    print result

                    if(result.task.filename == name):

                        noofchuncks = result.task.noofchuncks

                        print noofchuncks

                    while noofchuncks != 0:

                        if (result.task.filename == name):

                            with open(path/name, "w") as outfile:

                                outfile.write(result.task.fileContent)
                    
                elif (choice == 3):
                    print("");
                    print("-----------------------------------------------\n");
                    print("1.) Upload - Upload a New File \n");
                    print("2.) Download - To Download a particular File\n");
                    print("3.) help - list the options\n");
                    print("4.) exit - end session\n");
                    print("\n");
                else:
                    print("Wrong Selection");
        print("\nGoodbye\n");
        
        
if __name__ == '__main__':
    ca = BasicClientApp();
    ca.run();