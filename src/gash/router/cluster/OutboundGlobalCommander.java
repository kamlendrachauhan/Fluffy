package gash.router.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.QueueManager;
import gash.router.server.model.GlobalCommandMessageChannelCombo;
import io.netty.channel.ChannelFuture;

public class OutboundGlobalCommander extends Thread {

	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(OutboundGlobalCommander.class);

	public OutboundGlobalCommander(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.getOutboundGlobalCommandQ() == null)
			throw new RuntimeException("Manager has a null queue");
	}

	@Override
	public void run() {
		while (true) {

			try {
				// block until a message is enqueued
				GlobalCommandMessageChannelCombo msg = this.manager.dequeueGlobalOutboundCommmand();

				if (logger.isDebugEnabled())
					logger.debug("Outbound management message routing to node "
							+ msg.getGlobalCommandMessage().getHeader().getDestination());

				if (msg.getChannel() != null && msg.getChannel().isOpen()) {
					boolean rtn = false;
					if (msg.getChannel().isWritable()) {
						ChannelFuture cf = msg.getChannel().writeAndFlush(msg.getGlobalCommandMessage());

						cf.awaitUninterruptibly();
						rtn = cf.isSuccess();
						if (!rtn)
							manager.returnGlobalOutboundCommand(msg);
					}

				} else {
					// logger.info("channel to node " +
					// msg.getCommandMessage().getHeader().getDestination() + "
					// is not writable");
					manager.returnGlobalOutboundCommand(msg);
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
