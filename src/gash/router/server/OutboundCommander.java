package gash.router.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.model.CommandMessageChannelCombo;
import io.netty.channel.ChannelFuture;

public class OutboundCommander extends Thread{

	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger("OutboundQueueWorker");
	
	public OutboundCommander(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.outboundCommQ == null)
			throw new RuntimeException("Manager has a null queue");
	}
	
	@Override
	public void run() {
		while (true) {

			try {
				// block until a message is enqueued
				CommandMessageChannelCombo msg = this.manager.dequeueOutboundCommmand();
				/**
				 * Incrementing the no. of processed tasks
				 */
				//NodeState.getInstance().incrementProcessed();
				
				if (logger.isDebugEnabled())
					logger.debug("Outbound management message routing to node " + msg.getCommandMessage().getHeader().getDestination());

				if (msg.getChannel()!= null && msg.getChannel().isOpen()) {
					boolean rtn = false;
					if (msg.getChannel().isWritable()) {
						ChannelFuture cf = msg.getChannel().write(msg.getCommandMessage());
						msg.getChannel().flush();
						cf.awaitUninterruptibly();
						rtn = cf.isSuccess();
						if (!rtn)
							manager.returnOutboundCommand(msg);
					}

				} else {
					//logger.info("channel to node " + msg.getCommandMessage().getHeader().getDestination() + " is not writable");
					manager.returnOutboundCommand(msg);
				}
			} catch (InterruptedException ie) {
				break;
			} catch (Exception e) {
				logger.error("Unexpected management communcation failure", e);
				break;
			}
		}
	}
}
