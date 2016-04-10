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

import pipe.common.Common.Header;
import pipe.monitor.Monitor.ClusterMonitor;
import routing.Pipe.CommandMessage;

public class MonitorClientApp implements MonitorListener{
	private MonitorClient mc;
	private static int count = 0;
	
	public MonitorClientApp(MonitorClient mc) {
		init(mc);
	}

	private void init(MonitorClient mc) {
		this.mc = mc;
		this.mc.addListener(this);
	}
	
	public ClusterMonitor sendDummyMessage() {
		/*
		 * This message should be created and sent by only one node inside the cluster.
		 */
		
		//Build the message to be sent to monitor server
		ClusterMonitor.Builder cm = ClusterMonitor.newBuilder();
		//your cluster ID
		cm.setClusterId(420);
		//No of nodes in your cluster
		cm.setNumNodes(4);
		//Node Id = Process Id
		
		cm.addProcessId(1);
		cm.addProcessId(2);
		cm.addProcessId(3);
		cm.addProcessId(4);
		//Set processId,No of EnquedTask for that processId
		cm.addEnqueued(0);
		cm.addEnqueued(0);
		cm.addEnqueued(0);
		cm.addEnqueued(1);
		//Set processId,No of ProcessedTask for that processId
		cm.addProcessed(2);
		cm.addProcessed(1);
		cm.addProcessed(0);
		cm.addProcessed(0);
		//Set processId,No of StolenTask for that processId
		cm.addStolen(0);
		cm.addStolen(0);
		cm.addStolen(0);
		cm.addStolen(0);
		//Increment tick every time you send the message, or else it would be ignored
		// Tick starts from 0
		cm.setTick(count++);
		
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(4);
		hb.setTime(System.currentTimeMillis());
		
		
		return cm.build();
	}
	
	@Override
	public String getListenerID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void onMessage(CommandMessage cmdMsg) {
		if(cmdMsg.hasMonitorMsg()){
			ClusterMonitor msg = cmdMsg.getMonitorMsg();

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
	}
	
	public static void main(String[] args) {
		/*
		 * Set host and port of Monitor Server
		 */
		String host = "192.168.1.22";
		int port = 5000;

		try {
			
			MonitorClient mc = new MonitorClient(host, port);
			MonitorClientApp ma = new MonitorClientApp(mc);
						
			
			// do stuff w/ the connection
			System.out.println("Creating message");
			//Send a dummy monitor message to a node in our cluster
			ClusterMonitor msg = ma.sendDummyMessage();
			System.out.println("Sending generated message");
			mc.write(msg);
			
			//Get the load of the node and send it to the Monitor Server
			
			
			System.out.println("\n** exiting in 10 seconds. **");
			System.out.flush();
			Thread.sleep(10 * 10000);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			
		}
	}

}
