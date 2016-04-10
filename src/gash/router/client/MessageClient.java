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
package gash.router.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.google.protobuf.ByteString;

import pipe.common.Common.Header;
import pipe.common.Common.Task;
import pipe.common.Common.Task.TaskType;
import routing.Pipe.CommandMessage;

/**
 * front-end (proxy) to our service - functional-based
 * 
 * @author gash
 * 
 */
public class MessageClient {
	// track requests
	private long curID = 0;
	private static int seriesCounter =0;
	private static int seqID = 1;

	public MessageClient(String host, int port) {
		init(host, port);
	}

	private void init(String host, int port) {
		CommConnection.initConnection(host, port);
	}

	public void addListener(CommListener listener) {
		CommConnection.getInstance().addListener(listener);
	}

	public void ping() {
		// construct the message to send
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setPing(true);

		try {
			// direct no queue
			// CommConnection.getInstance().write(rb.build());

			// using queue
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void sendReadRequest(String filename) throws UnknownHostException{
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);
		
		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		
		Task.Builder tb = Task.newBuilder();
		tb.setFilename(filename);
		tb.setTaskType(TaskType.READ);
		tb.setSender(InetAddress.getLocalHost().getHostAddress());
		
		rb.setMessage(filename);
		/*tb.setSeqId(seqID++);
		tb.setSeriesId(seriesCounter++);*/
		
		rb.setTask(tb.build());
		
		try {
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public void release() {
		CommConnection.getInstance().release();
	}

	/**
	 * Since the service/server is asychronous we need a unique ID to associate
	 * our requests with the server's reply
	 * 
	 * @return
	 */
	private synchronized long nextId() {
		return ++curID;
	}
	
	public void sendFile(ByteString bs, String filename, int numOfChunks, int chunkId) {
		// construct the message to send
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);
		
		Task.Builder tb = Task.newBuilder();
		tb.setNoOfChunks(numOfChunks); //Num of chunks
		tb.setChunkNo(chunkId);    //chunk id
		tb.setTaskType(Task.TaskType.WRITE);
		try {
			tb.setSender(InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		tb.setFilename(filename);
		tb.setChunk(bs);
		
		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setTask(tb);
		rb.setMessage(filename);
		
		try {
			// direct no queue
			// CommConnection.getInstance().write(rb.build());

			// using queue
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
