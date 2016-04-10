/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.app;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.google.protobuf.ByteString;

import gash.router.client.CommConnection;
import gash.router.client.CommListener;
import gash.router.client.MessageClient;
import routing.Pipe;
import routing.Pipe.CommandMessage;

public class DemoApp implements CommListener {
	private MessageClient mc;
	private Map<String, ArrayList<CommandMessage>> byteList = new HashMap<String, ArrayList<CommandMessage>>();

	public DemoApp(MessageClient mc) {
		init(mc);
	}

	private void init(MessageClient mc) {
		this.mc = mc;
		this.mc.addListener(this);
	}

	private void ping(int N) {
		// test round-trip overhead (note overhead for initial connection)
		final int maxN = 10;
		long[] dt = new long[N];
		long st = System.currentTimeMillis(), ft = 0;
		for (int n = 0; n < N; n++) {
			mc.ping();
			ft = System.currentTimeMillis();
			dt[n] = ft - st;
			st = ft;
		}

		System.out.println("Round-trip ping times (msec)");
		for (int n = 0; n < N; n++)
			System.out.print(dt[n] + " ");
		System.out.println("");
	}

	@Override
	public String getListenerID() {
		return "demo";
	}

	@Override
	public void onMessage(CommandMessage msg) {
		//System.out.println("---> " + msg+" ---Message : "+msg.getMessage());
		if(msg.hasTask()){
			/*System.out.println(msg.getTask().getFilename());
			System.out.println(msg.getTask().getChunkNo());
			System.out.println(msg.getTask().getNoOfChunks());*/
			
			
			if(!byteList.containsKey(msg.getTask().getFilename())){
				byteList.put(msg.getTask().getFilename(), new ArrayList<Pipe.CommandMessage>());
				System.out.println("Chunk list created ");
			}
			byteList.get(msg.getTask().getFilename()).add(msg);
			if(byteList.get(msg.getTask().getFilename()).size() == msg.getTask().getNoOfChunks()){
				
				
				try {
					File file = new File(msg.getTask().getFilename());
					file.createNewFile();
					List<ByteString> byteString = new ArrayList<ByteString>();
					FileOutputStream outputStream = new FileOutputStream(file);
					int i=1;
					while(i <= msg.getTask().getNoOfChunks()){
						for(int j=0; j < byteList.get(msg.getTask().getFilename()).size(); j++){
							System.out.println("Inside the for loop ");
							if(byteList.get(msg.getTask().getFilename()).get(j).getTask().getChunkNo() == i){
								System.out.println("Added chunk to file "+i);
								byteString.add(byteList.get(msg.getTask().getFilename()).get(j).getTask().getChunk());
								System.out.println(byteList.get(msg.getTask().getFilename()).get(j).getTask().getChunk().size());
								//outputStream.write(byteList.get(msg.getTask().getFilename()).get(j).getTask().getChunk().toByteArray());
								
								i++;
								break;
							}
						}
						
					}
					ByteString bs = ByteString.copyFrom(byteString);
					outputStream.write(bs.toByteArray());
					outputStream.flush();
					outputStream.close();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
		System.out.flush();
	}
	
	
/*
	*//**
	 * sample application (client) use of our messaging service
	 * 
	 * @param args
	 *//*
	public static void main(String[] args) {
		String host = "192.168.1.22";
		int port = 5101;

		try {
			MessageClient mc = new MessageClient(host, port);
			DemoApp da = new DemoApp(mc);

			// do stuff w/ the connection
			//da.ping(2);
			//da.sendReadTasks();
			da.splitFile(new File("introductions3.pdf"));

			System.out.println("\n** exiting in 10 seconds. **");
			System.out.flush();
			Thread.sleep(10 * 100000);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CommConnection.getInstance().release();
		}
	}*/
/*	
	public void sendReadTasks(){
		try {
			mc.sendReadRequest("introductions3.pdf");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}*/
	/**
	 * sample application (client) use of our messaging service
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		Scanner s = new Scanner(System.in);
		boolean isExit = false;
		System.out.println("Welcome to Fluffy!\n Enter the leader's IP address :");


		try {

			String host = s.nextLine();
			System.out.println("Enter the leader's port number : ");
			int port = s.nextInt();
			MessageClient mc = new MessageClient(host, port);
			DemoApp da = new DemoApp(mc);
			int choice = 0;

			while (true) {
				System.out.println("Enter your option \n1. WRITE a file. \n2. READ a file. \n3. Show the menu. \n4. Exit");
				choice = s.nextInt();
				switch (choice) {
				case 1:{
					System.out.println("Enter the full pathname of the file to be written ");

					String currFileName = s.next();
					File file = new File(currFileName);
					if(file.exists()){
						da.splitFile(file);
						Thread.sleep(10*1000);
					}else{
						throw new FileNotFoundException("File does not exist in this path ");
					}
				}

				break;

				case 2:{
					System.out.println("Enter the file name to be read : ");
					String currFileName = s.nextLine();
					da.sendReadTasks(currFileName);
					Thread.sleep(1000*100);
				}
				break;

				case 3:
					break;

				case 4:
					isExit = true;
					break;

				default:
					break;
				}
				if(isExit)
					break;
			}

			/*


			// do stuff w/ the connection
			//da.ping(2);
			//da.sendReadTasks();
			da.splitFile(new File("introductions3.pdf"));

			System.out.println("\n** exiting in 10 seconds. **");
			System.out.flush();
			Thread.sleep(10 * 100000);
			 */} catch (Exception e) {
				 e.printStackTrace();
			 } finally {
				 CommConnection.getInstance().release();
				 if(s != null)
					 s.close();
			 }
	}

	public void sendReadTasks(String filename){
		try {
			mc.sendReadRequest(filename);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	private void splitFile(File f) throws IOException {
    	ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();
        
        int sizeOfFiles = 1024 * 1024;// 1MB
        int numOfChunks = 0;
        byte[] buffer = new byte[sizeOfFiles];

        try {
        	BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f));
            String name = f.getName();

            int tmp = 0;
            while ((tmp = bis.read(buffer)) > 0) {
                try {
                	ByteString bs = ByteString.copyFrom(buffer, 0, tmp);
                	chunkedFile.add(bs);
                	numOfChunks++;
                }
                catch (Exception ex) {
        			ex.printStackTrace();
        		} 
            }
            
            for(int x=0;x<chunkedFile.size();x++){
            	mc.sendFile(chunkedFile.get(x), name,numOfChunks,x+1);   //x -> chunk id
            }
            
        }
        catch (Exception ex) {
			ex.printStackTrace();
		}
        System.out.println(chunkedFile.size());
        
        
        
    }

}
