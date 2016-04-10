package gash.router.server;

import java.rmi.UnexpectedException;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.cache.RiakHandler;
import gash.router.persistence.DatabaseHandler;
import gash.router.persistence.replication.DataReplicationManager;
import gash.router.raft.leaderelection.ElectionManagement;
import gash.router.server.model.CommandMessageChannelCombo;
import gash.server.util.MessageGeneratorUtil;
import global.Global.GlobalCommandMessage;
import io.netty.channel.Channel;
import pipe.common.Common.Task;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class InboundCommander extends Thread {

	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(InboundCommander.class);

	public InboundCommander(QueueManager poller) {
		super();
		this.manager = poller;
		if (poller.inboundCommQ == null)
			throw new RuntimeException("Poller has a null queue");
	}

	@Override
	public void run() {

		// Poll the queue for messages
		while (true) {
			boolean isSuccess = false;
			try {
				CommandMessageChannelCombo currCombo = manager.dequeueInboundCommmand();
				Channel currChannel = currCombo.getChannel();
				CommandMessage currMsg = currCombo.getCommandMessage();
				Task currTask = currCombo.getCommandMessage().getTask();
				switch (currTask.getTaskType()) {
				case WRITE:
					// Write it to this(master) node. Send ACK to the client and
					// asynchronously start replication on the remaining
					// servers.

					// Writing to itself
					try {

						// If file size has only 1 chunk, write to in memory DB,
						// else write to the standard DB.
						if (currTask.getNoOfChunks() == 1) {

							RiakHandler.storeFile(currTask.getFilename(), currMsg.getTask().getChunk().toByteArray());

						} else {
							// TODO Write to standard DB
							DatabaseHandler.addFile(currTask.getFilename(), currMsg.getTask().getChunk().toByteArray(),
									currTask.getNoOfChunks(), currTask.getChunkNo());
							isSuccess = true;
						}

					} catch (Exception e) {
						e.printStackTrace();
						logger.error(e.getMessage());
					}

					// Starting asynchronous replication
					DataReplicationManager.getInstance().replicate(currMsg);

					// Send ACK to the client
					CommandMessage response = MessageGeneratorUtil.getInstance().generateClientResponseMsg(isSuccess);
					manager.enqueueOutboundCommand(response, currChannel);
					logger.info("Finished processing task " + currTask.getFilename() + " from client : "
							+ currChannel.remoteAddress());

					break;

				case READ:
					/*
					 * This is a command message, so this is directly from a
					 * client and this node is a leader. So find a node who can
					 * process this request. Then generate a work message to get
					 * the file from that node. Also, store the client channel
					 * so that we can send the read file to him directly.
					 */

					Channel nextChannel = NodeChannelManager.getNextReadChannel();

					// Get the no. of chunks in the file FROM ITS OWN DB,
					// although the file is to be read from another node.
					int chunkCount = DatabaseHandler.getChuncks(currMsg.getTask().getFilename());

					if (chunkCount == 0) {
						// File is not present in this cluster
						GlobalCommandMessage globalCommandMessage = MessageGeneratorUtil.getInstance()
								.generateReadRequestGlobalCommmandMessage(currMsg.getTask().getFilename(),
										NodeChannelManager.currentLeaderID);

						// Fetch the Global Command Message Adapters channel to
						// forward request
						Channel globalCommandAdapterChannel = NodeChannelManager.getGlobalCommandAdapterChannel();
						if (globalCommandAdapterChannel != null)
							logger.debug("The channel has been created with global command adapter channel");
						else {
							globalCommandAdapterChannel = ElectionManagement.globalChannel;
						}
						if (globalCommandAdapterChannel != null && globalCommandAdapterChannel.isActive()
								&& globalCommandAdapterChannel.isWritable()) {
							/*
							 * The client map generally saves requestID and
							 * Client channel mapping but in this case the map
							 * will store filename as the key
							 */
							ConcurrentHashMap<String, CommandMessageChannelCombo> clientChannelMap = NodeChannelManager
									.getClientChannelMap();
							clientChannelMap.put(currMsg.getTask().getFilename(), currCombo);
							manager.enqueueGlobalOutboundCommand(globalCommandMessage, globalCommandAdapterChannel);
						} else {
							logger.error(
									"The Global Channel is not available to handle " + globalCommandAdapterChannel);
						}
					} else if (currMsg.getTask().getNoOfChunks() == 1
							&& RiakHandler.getFile(currMsg.getTask().getFilename()) != null) {
						// Fetch from in-memory db

						// Generate proper command message to sent to client.
						CommandMessage message = MessageGeneratorUtil.getInstance().generateRiakFileMessage(
								RiakHandler.getFile(currMsg.getTask().getFilename()), currMsg.getTask().getFilename());

						// Writing directly back to client
						QueueManager.getInstance().enqueueOutboundCommand(message, currChannel);

					} else {
						// Setting the chunk count to be decremented each time
						// this file's chunk is sent back to the client.
						currCombo.setChunkCount(chunkCount);

						// Store the client channel so it can be used later to
						// reply back to the client.
						String requestId = NodeChannelManager.addClientToMap(currCombo);

						// Generate proper work message to send to next client.
						WorkMessage message = MessageGeneratorUtil.getInstance().generateDelegationMessage(currMsg,
								requestId);

						// Enqueue the generated message to the outbound work
						// queue
						manager.enqueueOutboundWork(message, nextChannel);
					}

					/*
					 * Thread.sleep(10000); logger.info(
					 * "Finished processing task "
					 * +currCombo.getCommandMessage().getTask().getFilename());
					 */
					break;

				default:
					break;
				}

			} catch (InterruptedException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			} catch (UnexpectedException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			} catch (Exception e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		}
	}

}
