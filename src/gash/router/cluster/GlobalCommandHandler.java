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
package gash.router.cluster;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.GlobalRoutingConf;
import gash.router.server.NodeChannelManager;
import gash.router.server.QueueManager;
import gash.server.util.MessageGeneratorUtil;
import global.Global.GlobalCommandMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Failure;
import pipe.work.Work.WorkMessage;

public class GlobalCommandHandler extends SimpleChannelInboundHandler<GlobalCommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger(GlobalCommandHandler.class);
	protected GlobalRoutingConf conf;

	public GlobalCommandHandler(GlobalRoutingConf conf) {

		if (conf != null) {
			this.conf = conf;
		}
	}

	/**
	 * override this method to provide processing behavior. This implementation
	 * mimics the routing we see in annotating classes to support a RESTful-like
	 * behavior (e.g., jax-rs).
	 * 
	 * @param msg
	 */
	public void handleMessage(GlobalCommandMessage msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}
		// TODO enable printUtil for Global messages
		// PrintUtil.printCommand(msg);
		// TODO How can you implement this without if-else statements?
		try {
			/*
			 * If the incoming request is read we should delegate it to leader
			 * or if the the request has come from the other cluster we should
			 * forward to leader
			 */
			if (msg.hasQuery()) {

				/**
				 * TODO Enqueue the command message and the channel into the
				 * server queue
				 */
				logger.info("Received task from " + msg.getHeader().getNodeId());
				System.out.println("Queuing task");
				System.out.flush();
				QueueManager.getInstance().enqueueGlobalInboundCommmand(msg, channel);
			} else if (msg.hasResponse()) {
				/*
				 * If the response has come for Read request, form a work
				 * message and forward the data to the leader to respond back to
				 * client
				 */
				if (msg.getResponse().hasData()) {
					WorkMessage reponseWorkMessage = MessageGeneratorUtil.getInstance()
							.convertGlobalCommandMessage2WorkMessage(msg);
					ConcurrentHashMap<Integer, Channel> node2ChannelMap = NodeChannelManager.getNode2ChannelMap();
					if (!node2ChannelMap.isEmpty() && node2ChannelMap.containsKey(NodeChannelManager.currentLeaderID)) {
						Channel leaderChannel = node2ChannelMap.get(NodeChannelManager.currentLeaderID);
						QueueManager.getInstance().enqueueOutboundWork(reponseWorkMessage, leaderChannel);
					} else {
						logger.error("The leader channel is not available in the map");
						// TODO try to create the channel to the leader
					}
				} else {
					logger.debug("The Node " + msg.getHeader().getNodeId() + " replied without any data");
				}

			} else if (msg.hasPing()) {
				logger.info("ping from " + msg.getHeader().getNodeId());
			} else if (msg.hasMessage()) {
				logger.info(msg.getMessage());
			}

		} catch (Exception e) {
			// TODO add logging
			try {

				Failure.Builder eb = Failure.newBuilder();
				eb.setId(conf.getNodeId());
				eb.setRefId(msg.getHeader().getNodeId());
				eb.setMessage(e.getMessage());
				GlobalCommandMessage.Builder rb = GlobalCommandMessage.newBuilder(msg);
				rb.setErr(eb);
				channel.writeAndFlush(rb.build());
			} catch (Exception e2) {
				e.printStackTrace();
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
	protected void channelRead0(ChannelHandlerContext ctx, GlobalCommandMessage msg) throws Exception {
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}