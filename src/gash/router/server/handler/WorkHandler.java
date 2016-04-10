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
package gash.router.server.handler;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.persistence.replication.DataReplicationManager;
import gash.router.raft.leaderelection.ElectionManagement;
import gash.router.raft.leaderelection.ElectionTImer;
import gash.router.server.NodeChannelManager;
import gash.router.server.QueueManager;
import gash.router.server.ServerState;
import gash.router.server.model.WorkMessageChannelCombo;
import gash.server.util.MessageBuilder;
import gash.server.util.PrintUtil;
import gash.server.util.RandomTimeoutGenerator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Failure;
import pipe.common.Common.Task.TaskType;
import pipe.election.Election.LeaderStatus.LeaderQuery;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.StateOfLeader;
import pipe.work.Work.WorkState;
import pipe.work.Work.WorkSteal;
import pipe.work.Work.WorkSteal.StealType;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class WorkHandler extends SimpleChannelInboundHandler<WorkMessage> {
	protected static Logger logger = LoggerFactory.getLogger("WorkHandler");
	protected ServerState state;
	protected boolean debug = false;
	// private static Timer electionTimer;
	private static ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();

	public WorkHandler(ServerState state) {
		// electionTimer = new Timer();

		if (state != null) {
			this.state = state;
		}
	}

	/**
	 * override this method to provide processing behavior. T
	 * 
	 * @param msg
	 */
	public void handleMessage(WorkMessage msg, Channel channel) {
		if (msg == null) {
			logger.error("ERROR: Unexpected content - " + msg);
			return;
		}

		if (debug)
			PrintUtil.printWork(msg);

		// TODO How can you implement this without if-else statements?
		try {

			if (msg.getStateOfLeader() == StateOfLeader.LEADERALIVE) {
				System.out.println("Leader is Alive ");

				exec.shutdownNow();
				int currentTimeout = RandomTimeoutGenerator.randTimeout() * state.getConf().getNodeId();
				exec = Executors.newSingleThreadScheduledExecutor();
				exec.schedule(new ElectionTImer(), (long) currentTimeout, TimeUnit.MILLISECONDS);
			} else if (msg.hasLeader() && msg.getLeader().getAction() == LeaderQuery.WHOISTHELEADER) {
				WorkMessage buildNewNodeLeaderStatusResponseMessage = MessageBuilder
						.buildNewNodeLeaderStatusResponseMessage(NodeChannelManager.currentLeaderID,
								NodeChannelManager.currentLeaderAddress);

				ChannelFuture cf = channel.write(buildNewNodeLeaderStatusResponseMessage);
				channel.flush();
				cf.awaitUninterruptibly();
				if (cf.isDone() && !cf.isSuccess()) {
					logger.info("Failed to write the message to the channel ");
				}

				// Sent the newly discovered node all the data on this node.
				logger.info("Attempting to auto-replicate to node : " + channel.remoteAddress().toString());
				DataReplicationManager.getInstance().replicateToNewNode(channel);

			} else if (msg.hasLeader() && msg.getLeader().getAction() == LeaderQuery.THELEADERIS) {
				NodeChannelManager.currentLeaderID = msg.getLeader().getLeaderId();
				NodeChannelManager.currentLeaderAddress = msg.getLeader().getLeaderHost();
				NodeChannelManager.amIPartOfNetwork = true;
				logger.info("The leader is " + NodeChannelManager.currentLeaderID);
			}

			if (msg.hasBeat() && msg.getStateOfLeader() != StateOfLeader.LEADERALIVE) {

				logger.info("heartbeat received from " + msg.getHeader().getNodeId());

			} else if (msg.hasSteal()) {

				switch (msg.getSteal().getStealtype()) {
				case STEAL_RESPONSE:

					logger.info("------Stealing work from node:------ " + msg.getHeader().getNodeId());
					QueueManager.getInstance().enqueueInboundWork(msg, channel);
					logger.info("------A task was stolen from another node------");

					break;

				case STEAL_REQUEST:

					WorkMessageChannelCombo msgOnQueue = QueueManager.getInstance().getInboundWorkQ().peek();

					if (msgOnQueue != null && msgOnQueue.getWorkMessage().getTask().getTaskType() == TaskType.READ) {
						logger.info("------Pending Read Request found in Queue, Sending to another node------");
						WorkMessage wmProto = QueueManager.getInstance().dequeueInboundWork().getWorkMessage();
						WorkMessage.Builder wm = WorkMessage.newBuilder(wmProto);
						WorkSteal.Builder stealMessage = WorkSteal.newBuilder();
						stealMessage.setStealtype(StealType.STEAL_RESPONSE);
						wm.setSteal(stealMessage);
						QueueManager.getInstance().enqueueOutboundWork(wm.build(), channel);
					}

					break;

				default:
					break;
				}

			} else if (msg.hasTask()) {
				// Enqueue it to the inbound work queue
				logger.info("Received inbound work ");
				QueueManager.getInstance().enqueueInboundWork(msg, channel);

			} else if (msg.hasFlagRouting()) {
				logger.info("Routing information recieved " + msg.getHeader().getNodeId());
				logger.info("Routing Entries: " + msg.getRoutingEntries());

				logger.debug("Connected to new node");
				SocketAddress remoteAddress = channel.remoteAddress();
				InetSocketAddress addr = (InetSocketAddress) remoteAddress;

				state.getEmon().createOutboundIfNew(msg.getHeader().getNodeId(), addr.getHostName(), 5100);

				System.out.println(addr.getHostName());

			} else if (msg.hasNewNode()) {
				logger.info("NEW NODE TRYING TO CONNECT " + msg.getHeader().getNodeId());
				WorkMessage wm = state.getEmon().createRoutingMsg();

				ChannelFuture cf = channel.write(wm);
				channel.flush();
				cf.awaitUninterruptibly();
				if (cf.isDone() && !cf.isSuccess()) {
					logger.info("Failed to write the message to the channel ");
				}
				// TODO New node has been detected. Push all your data to it
				// now.
				SocketAddress remoteAddress = channel.remoteAddress();
				InetSocketAddress addr = (InetSocketAddress) remoteAddress;

				state.getEmon().createInboundIfNew(msg.getHeader().getNodeId(), addr.getHostName(), 5100);
				state.getEmon().getInboundEdges().getNode(msg.getHeader().getNodeId()).setChannel(channel);

			} else if (msg.hasPing()) {
				logger.info("ping from " + msg.getHeader().getNodeId());
				boolean p = msg.getPing();
				WorkMessage.Builder rb = WorkMessage.newBuilder();
				rb.setPing(true);
				// channel.writeAndFlush(rb.build());
			} else if (msg.getHeader().hasElection() && msg.getHeader().getElection()) {
				// call the election handler to handle this request
				System.out.println(" ---- Message for election has come ---- ");
				ElectionManagement.processElectionMessage(channel, msg);
			} else if (msg.hasErr()) {
				Failure err = msg.getErr();
				logger.error("failure from " + msg.getHeader().getNodeId());
				// PrintUtil.printFailure(err);
			} else if (msg.hasState()) {
				WorkState s = msg.getState();
			} else {
				logger.info("Executing from work handler ");

			}
		} catch (NullPointerException e) {
			logger.error("Null pointer has occured" + e.getMessage());
		}

		catch (Exception e) {
			// TODO add logging
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(state.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			WorkMessage.Builder rb = WorkMessage.newBuilder(msg);
			rb.setErr(eb);

			ChannelFuture cf = channel.write(rb.build());
			channel.flush();
			cf.awaitUninterruptibly();
			if (cf.isDone() && !cf.isSuccess()) {
				logger.info("Failed to write the message to the channel ");
			}
		}

		System.out.flush();

	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, WorkMessage msg) throws Exception {
		Channel channel = ctx.channel();
		handleMessage(msg, channel);

		// Thread.sleep(300);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		logger.error("Node CHANNEL WAS DESTROYED: " + ctx.channel().remoteAddress());
		InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
		InetAddress inetAddress = socketAddress.getAddress();
		logger.error(inetAddress.getHostAddress());
		// state.getEmon().getOutboundEdges().removeNodeByIp(inetAddress.getHostAddress());
	}

}