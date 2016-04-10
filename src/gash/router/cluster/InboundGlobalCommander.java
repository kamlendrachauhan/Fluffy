package gash.router.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.persistence.DatabaseHandler;
import gash.router.persistence.MessageDetails;
import gash.router.persistence.replication.DataReplicationManager;
import gash.router.server.NodeChannelManager;
import gash.router.server.QueueManager;
import gash.router.server.model.GlobalCommandMessageChannelCombo;
import gash.server.util.MessageGeneratorUtil;
import global.Global.GlobalCommandMessage;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;
import storage.Storage.Action;

public class InboundGlobalCommander extends Thread {

	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(InboundGlobalCommander.class);

	public InboundGlobalCommander(QueueManager poller) {
		super();
		this.manager = poller;
		if (poller.getInboundGlobalCommandQ() == null)
			throw new RuntimeException("Poller has a null queue");
	}

	@Override
	public void run() {

		// Poll the queue for messages
		while (true) {
			boolean isSuccess = false;
			try {
				GlobalCommandMessageChannelCombo currCombo = manager.dequeueGlobalInboundCommmand();
				Channel currChannel = currCombo.getChannel();
				GlobalCommandMessage globalCommandMessage = currCombo.getGlobalCommandMessage();
				Action currentAction = globalCommandMessage.getQuery().getAction();
				switch (currentAction) {
				case STORE:
					// Write it to this(master) node. Send ACK to the
					// client and asynchronously start replication on the
					// remaining servers.
					try {
						DatabaseHandler.addFile(globalCommandMessage.getQuery().getKey(),
								globalCommandMessage.getQuery().getData().toByteArray(),
								globalCommandMessage.getQuery().getMetadata().getSeqSize(),
								globalCommandMessage.getQuery().getSequenceNo());
						isSuccess = true;
					} catch (Exception e) {
						logger.error("An Error has occured while writing to database");
					}

					CommandMessage commandMsg = MessageGeneratorUtil.getInstance()
							.convertGlobalCommandMessage2CommandMessage(globalCommandMessage);
					DataReplicationManager.getInstance().replicate(commandMsg);
					
					//ACK to the remote cluster 
					GlobalCommandMessage responseToWrite = MessageGeneratorUtil.getInstance()
							.generateWriteResponseToGlobalCommandAdapter(globalCommandMessage, isSuccess);
					manager.enqueueGlobalOutboundCommand(responseToWrite, currChannel);
					logger.info("Finished processing task " + globalCommandMessage.getQuery().getKey()
							+ " from other cluster : " + currChannel.remoteAddress());

					break;

				case GET:
					/*
					 * This is global command message and might come from either
					 * leader in the current cluster or from adapters from other
					 * clusters. If nodeId in header is set to the current
					 * leaders id it means that is from leader otherwise its
					 * from outside the cluster.
					 */

					if (globalCommandMessage.getHeader().getNodeId() == NodeChannelManager.currentLeaderID) {
						// If the call has come here it means that the file
						// requested is not present in current
						// cluster's storage. BroadCast a GlobalCommandMessage
						// to all the outboundEdges.
						if (globalCommandMessage.getQuery().hasKey()) {

							String requestedFileName = globalCommandMessage.getQuery().getKey();
							GlobalCommandMessage broadcastGlobalCommandMsg = MessageGeneratorUtil.getInstance()
									.generateReadRequestGlobalCommmandMessage(requestedFileName,
											NodeChannelManager.currentLeaderID);
							ClusterNodeChannelManager.broadcastGlobalCommandMessage(broadcastGlobalCommandMsg);
							// TODO once the response comes back in
							// GlobalCommandHandler sends the data to the leader
						} else {
							logger.error("The file name does is not set as part of the request");
							// TODO respond back with proper error message
							break;
						}
					} else {
						// Call has come here as the message is arrived from
						// another cluster looking for file
						// Forward message to the same nodes work port
						String fileName = globalCommandMessage.getQuery().getKey();
						logger.info(" Received msg to read a file on this system ");
						int chunkCount = DatabaseHandler.getChuncks(fileName);
						if (chunkCount == 0) {
							// File is not available in this cluster, send a
							// response mentioning file could not be located on
							// this node
							GlobalCommandMessage noFileFoundMessage = MessageGeneratorUtil.getInstance()
									.generateReadFailResponseGlobalCommmandMessage(fileName);

							// Directly respond to the adapter with fail message
							currChannel.writeAndFlush(noFileFoundMessage);
						} else {
							for (int index = 1; index <= chunkCount; index++) {
								// Get the file chunks
								MessageDetails details = DatabaseHandler.getFilewithChunckId(fileName, index);

								// Construct a work message for each chunk. Set
								// the destination as the leader. Set the
								// message type as SLAVE_READ_DONE
								GlobalCommandMessage msg = MessageGeneratorUtil.getInstance()
										.generateReadResonseGlobalCommmandMessage(fileName, details.getByteData(),
												index, chunkCount);
								// Send these messages to the outbound global
								// command queue, to be sent to same requesting
								// adapter
								manager.enqueueGlobalOutboundCommand(msg, currChannel);
							}
						}
					}
					break;

				default:
					break;
				}

			} catch (InterruptedException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			} catch (Exception e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		}
	}

}
