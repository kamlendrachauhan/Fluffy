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
package gash.router.server.edges;

import java.net.InetAddress;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.discovery.NodeDiscoveryManager;
import gash.router.raft.leaderelection.NodeState;
import gash.router.server.NodeChannelManager;
import gash.router.server.ServerState;
import gash.router.server.WorkInit;
import gash.server.util.MessageBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import pipe.common.Common.Header;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.StateOfLeader;
import pipe.work.Work.WorkState;

public class EdgeMonitor implements EdgeListener, Runnable {
	protected static Logger logger = LoggerFactory.getLogger("edge monitor");

	private static EdgeList outboundEdges;
	private static EdgeList inboundEdges;
	private long dt = 2000;
	private ServerState state;
	private boolean forever = true;
	private ArrayList<InetAddress> liveIps;
	private NodeState nodeState;

	public EdgeMonitor(ServerState state) {
		if (state == null)
			throw new RuntimeException("state is null");

		outboundEdges = new EdgeList();
		inboundEdges = new EdgeList();
		this.state = state;
		this.state.setEmon(this);

		if (state.getConf().getRouting() != null) {
			for (RoutingEntry e : state.getConf().getRouting()) {
				outboundEdges.addNode(e.getId(), e.getHost(), e.getPort());
			}
		}

		/// WHEN a new node enters it will have no routing entries.
		if (outboundEdges.isEmpty() && inboundEdges.isEmpty()) {
			NodeChannelManager.amIPartOfNetwork = false;
			System.out.println("No routing entries..possibly a new node");
			try {
				liveIps = NodeDiscoveryManager.checkHosts();
				Channel discoveryChannel = null;

				for (InetAddress oneIp : liveIps) {
					// Ignore own IP address
					if (!oneIp.getHostAddress().equals(InetAddress.getLocalHost().getHostAddress())) {
						System.out.println("Potential Server Node found of Network.. Trying to connect to:  "
								+ oneIp.getHostAddress());
						try {

							Channel newNodeChannel = connectToChannel(oneIp.getHostAddress(), 5100, this.state);

							if (newNodeChannel.isOpen() && newNodeChannel != null) {

								System.out.println("Channel connected to: " + oneIp.getHostAddress());
								WorkMessage wm = createNewNode();
								
								ChannelFuture cf = newNodeChannel.write(wm);
								newNodeChannel.flush();
								cf.awaitUninterruptibly();
								if (cf.isDone() && !cf.isSuccess()) {
									logger.info("Failed to write the message to the channel ");
								}
								if (discoveryChannel == null) {
									logger.info("Setting discovery channel for : "+oneIp.getHostAddress());
									discoveryChannel = newNodeChannel;
								}
							}
						} catch (Exception e) {
							System.out.println("Unable to connect to the potential client: " + oneIp.getHostAddress());
						}
					}

				}

				if (discoveryChannel != null) {
					// Send a discovery message to the first node that the new
					// node finds
					WorkMessage discoveryMessage = MessageBuilder.buildNewNodeLeaderStatusMessage();
					logger.info("I'm a new node. I shall now try to get the data from the cluster automatically");
					ChannelFuture cf = discoveryChannel.write(discoveryMessage);
					discoveryChannel.flush();
					cf.awaitUninterruptibly();
					if (cf.isDone() && !cf.isSuccess()) {
						logger.info("Failed to write the message to the channel ");
					}
					logger.debug("Writing to a node to get the leader status");
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		// cannot go below 2 sec
		if (state.getConf().getHeartbeatDt() > this.dt)
			this.dt = state.getConf().getHeartbeatDt();
	}

	public void createInboundIfNew(int ref, String host, int port) {
		inboundEdges.createIfNew(ref, host, port);
		System.out.println("--- Inbound Edges : " + inboundEdges.map.toString());

	}

	public void createOutboundIfNew(int ref, String host, int port) {
		outboundEdges.createIfNew(ref, host, port);
	}

	public WorkMessage createRoutingMsg() {

		pipe.work.Work.RoutingConf.Builder rb = pipe.work.Work.RoutingConf.newBuilder();

		ArrayList<String> ipList = new ArrayList<String>();
		ArrayList<String> idList = new ArrayList<String>();

		for (RoutingEntry destIp : state.getConf().getRouting()) {
			ipList.add(destIp.getHost());
			idList.add(destIp.getId() + "");
		}
		rb.addAllNodeId(idList);
		rb.addAllNodeIp(ipList);

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setSecret(1234);
		wb.setFlagRouting(true);
		wb.setRoutingEntries(rb);
		// TODO Is the leader really alive?
		wb.setStateOfLeader(StateOfLeader.LEADERKNOWN);
		return wb.build();
	}

	private WorkMessage createNewNode() {

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setNewNode(true);
		wb.setSecret(1234);
		wb.setStateOfLeader(StateOfLeader.LEADERUNKNOWN);
		return wb.build();
	}

	private WorkMessage createHB(EdgeInfo ei) {
		WorkState.Builder sb = WorkState.newBuilder();
		sb.setEnqueued(-1);
		sb.setProcessed(-1);

		Heartbeat.Builder bb = Heartbeat.newBuilder();
		bb.setState(sb);

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setBeat(bb);
		wb.setSecret(1234);
		addLeaderFieldToWorkMessage(wb);
		return wb.build();
	}

	private void addLeaderFieldToWorkMessage(WorkMessage.Builder wb) {
		if (NodeChannelManager.currentLeaderID == 0) {
			wb.setStateOfLeader(StateOfLeader.LEADERUNKNOWN);
		} else if (NodeChannelManager.currentLeaderID == this.state.getConf().getNodeId()) {
			// Current Node is the leader
			wb.setStateOfLeader(StateOfLeader.LEADERALIVE);
		} else {
			wb.setStateOfLeader(StateOfLeader.LEADERKNOWN);
		}
	}

	public void shutdown() {
		forever = false;
	}

	@Override
	public void run() {
		while (forever) {

			try {
				for (EdgeInfo ei : outboundEdges.map.values()) {
					try {
						// System.out.println("Inside For loop" +
						// outboundEdges.map.toString());
						if (ei.isActive() && ei.getChannel() != null) {
							if (NodeChannelManager.currentLeaderID == this.state.getConf().getNodeId()) {
								WorkMessage wm = createHB(ei);
								
								ChannelFuture cf = ei.getChannel().write(wm);
								ei.getChannel().flush();
								cf.awaitUninterruptibly();
								/*if (cf.isDone() && !cf.isSuccess()) {
									logger.info("Failed to write the message to the channel ");
								}*/
								logger.debug("Forming and sending out WorkMessage");
							}
							// System.out.println("Connected to Channel with
							// host :" + ei.getHost());
						} else if (ei.getChannel() == null) {
							Channel channel = connectToChannel(ei.getHost(), ei.getPort(), this.state);
							ei.setChannel(channel);
							// System.out.println("Connected to Channel with
							// host " + ei.getHost());
							ei.setActive(true);
							if (channel == null) {
								logger.info("trying to connect to node " + ei.getRef());
							}
						}
					} catch (Exception e) {
						System.out.println(e);
						Thread.sleep(dt);
						continue;
					}
				}
				Thread.sleep(dt);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private Channel connectToChannel(String host, int port, ServerState state) {
		Bootstrap b = new Bootstrap();
		NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup();
		WorkInit workInit = new WorkInit(state, false);

		try {
			b.group(nioEventLoopGroup).channel(NioSocketChannel.class).handler(workInit);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			// Make the connection attempt.
		} catch (Exception e) {
			logger.error("Could not connect to the host " + host);
			return null;
		}
		return b.connect(host, port).syncUninterruptibly().channel();

	}

	public static EdgeList getOutboundEdges() {
		return outboundEdges;
	}

	public static void setOutboundEdges(EdgeList outboundEdges) {
		EdgeMonitor.outboundEdges = outboundEdges;
	}

	public static EdgeList getInboundEdges() {
		return inboundEdges;
	}

	public static void setInboundEdges(EdgeList inboundEdges) {
		EdgeMonitor.inboundEdges = inboundEdges;
	}

	public ServerState getState() {
		return state;
	}

	public void setState(ServerState state) {
		this.state = state;
	}

	public NodeState getNodeState() {
		return nodeState;
	}

	public void setNodeState(NodeState nodeState) {
		this.nodeState = nodeState;
	}

	public void removeNodeByIp(String ip) {
		for (Integer curr : outboundEdges.map.keySet()) {
			if (outboundEdges.map.get(curr).getHost().equals(ip)) {
				outboundEdges.map.remove(curr);
				NodeChannelManager.node2ChannelMap.remove(curr);
			}
		}
	}

	@Override
	public synchronized void onAdd(EdgeInfo ei) {
		// TODO check connection
	}

	@Override
	public synchronized void onRemove(EdgeInfo ei) {
		// TODO ?
	}
}
