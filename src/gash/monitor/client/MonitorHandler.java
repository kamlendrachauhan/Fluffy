/*
 * copyright 2016, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.monitor.client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.monitor.Monitor.ClusterMonitor;

public class MonitorHandler extends SimpleChannelInboundHandler<ClusterMonitor> {

	
	static private int count = 0;
	protected ConcurrentMap<String, MonitorListener> listeners = new ConcurrentHashMap<String, MonitorListener>();
	public MonitorHandler() {
	}
	
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, ClusterMonitor msg) throws Exception {
		// TODO Auto-generated method stub
		handleMessage(msg, ctx.channel());
		
	}
	
	
	/*
	 * Do not assume how message are processed.
	 * More error handling conditions will be there for unwanted packets
	 * Follow the directions mentioned in dummyMessage created by client to create message 
	 * and send it to Monitor Server or else your message might get discarded
	 */
	protected void handleMessage(ClusterMonitor msg, Channel ch){
		if (msg == null) {
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}
		System.out.println("Count : "+count);
		count++;
		System.out.println("Tick : "+msg.getTick()+" Cluster ID :"+msg.getClusterId()+"  #Nodes : "+msg.getNumNodes()+"\nProcess ID's");
		for(int i=0;i<msg.getProcessIdCount();i++){
			System.out.print(msg.getProcessId(i)+" ");
		}
		System.out.println("\nNo of task enqueued");
		for(int i=0;i<msg.getEnqueuedCount();i++){
			System.out.print(msg.getEnqueued(i)+" ");
		}
		System.out.println("\nNo of task Processed");
		for(int i=0;i<msg.getProcessedCount();i++){
			System.out.print(msg.getProcessed(i));
		}
		System.out.println("\nNo of task Stolen");
		for(int i=0;i<msg.getStolenCount();i++){
			System.out.print(msg.getStolen(i)+" ");
		}
	}

	
	public void addListener(MonitorListener listener) {
		if (listener == null)
			return;

		listeners.putIfAbsent(listener.getListenerID(), listener);
	}

}