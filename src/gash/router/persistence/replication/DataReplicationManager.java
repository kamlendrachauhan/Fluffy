package gash.router.persistence.replication;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.persistence.DatabaseHandler;
import gash.router.persistence.MessageDetails;
import gash.router.server.NodeChannelManager;
import gash.router.server.QueueManager;
import gash.server.util.MessageGeneratorUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class DataReplicationManager {

	protected static Logger logger = LoggerFactory.getLogger("DataReplicationManager");

	protected static AtomicReference<DataReplicationManager> instance = new AtomicReference<DataReplicationManager>();

	public static DataReplicationManager initDataReplicationManager() {
		instance.compareAndSet(null, new DataReplicationManager());
		System.out.println(" --- Initializing Data Replication Manager --- ");
		return instance.get();
	}

	public static DataReplicationManager getInstance() throws Exception {
		if (instance != null && instance.get() != null) {
			return instance.get();
		}
		throw new Exception(" Data Replication Manager not started ");
	}

	public void replicateToNewNode(Channel channel) {

		logger.info("Started replicating to new node ");
		// DB handler returns a list of Work Messages with
		Map<String, ArrayList<MessageDetails>> fileMap = DatabaseHandler.getAllFilesForReplication();
		for (String filename : fileMap.keySet()) {
			for (MessageDetails details : fileMap.get(filename)) {
				try {
					WorkMessage workMessage = MessageGeneratorUtil.getInstance().generateNewNodeReplicationMsg(details,
							InetAddress.getLocalHost().getHostAddress());
					// channel.writeAndFlush(workMessage);

					ChannelFuture cf = channel.write(workMessage);
					channel.flush();
					cf.awaitUninterruptibly();
					if (cf.isDone() && !cf.isSuccess()) {
						logger.error("Failed to send replication message to server");
					}
				} catch (UnknownHostException e) {
					logger.error(e.getMessage());
				}
			}

		}
	}

	public void replicate(CommandMessage message) {
		ConcurrentHashMap<Integer, Channel> node2ChannelMap = NodeChannelManager.getNode2ChannelMap();
		if (node2ChannelMap != null && !node2ChannelMap.isEmpty()) {

			Set<Integer> keySet2 = node2ChannelMap.keySet();
			for (Integer nodeId : keySet2) {
				Channel nodeChannel = node2ChannelMap.get(nodeId);
				Replication replication = new Replication(message, nodeChannel, nodeId);
				Thread replicationThread = new Thread(replication);
				replicationThread.start();
			}

		}
	}

	// TODO convert this to Future and Callable
	private class Replication implements Runnable {
		private CommandMessage commandMessage;
		private Channel nodeChannel;
		private int nodeId;

		public Replication(CommandMessage commandMessage, Channel nodeChannel, Integer nodeId) {
			// Command Message here contains the chunk ID and the chunk nos. and
			// the chunk byte, in its Task field
			this.commandMessage = commandMessage;
			this.nodeChannel = nodeChannel;
			this.nodeId = nodeId;
		}

		@Override
		public void run() {
			if (this.nodeChannel.isOpen() && this.nodeChannel.isActive()) {
				// this.nodeChannel.writeAndFlush(replicationInfo);

				// Generate the work message to send to the slave nodes
				WorkMessage workMsg = MessageGeneratorUtil.getInstance().generateReplicationReqMsg(commandMessage,
						nodeId);

				// Push this message to the outbound work queue
				try {
					logger.info("Sending message for replication ");
					QueueManager.getInstance().enqueueOutboundWork(workMsg, nodeChannel);
				} catch (Exception e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			} else {
				logger.error("The nodeChannel to " + nodeChannel.localAddress() + " is not active");
			}
		}

	}
}
