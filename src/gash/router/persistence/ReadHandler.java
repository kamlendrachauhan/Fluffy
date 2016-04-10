package gash.router.persistence;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.NodeChannelManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.work.Work.WorkMessage;

public class ReadHandler {
	protected static Logger logger = LoggerFactory.getLogger("ReadHandler");

	protected static AtomicReference<ReadHandler> instance = new AtomicReference<ReadHandler>();

	public static ReadHandler initReadHandler() {
		instance.compareAndSet(null, new ReadHandler());
		System.out.println(" --- Initializing Read Handler --- ");
		return instance.get();
	}

	public static ReadHandler getInstance() {
		return instance.get();
	}

	public void readFile(WorkMessage workmessage) {
		ConcurrentHashMap<Integer, Channel> node2ChannelMap = NodeChannelManager.getNode2ChannelMap();
		if (node2ChannelMap != null && !node2ChannelMap.isEmpty()) {

			Set<Integer> keySet2 = node2ChannelMap.keySet();
			for (Integer nodeId : keySet2) {
				Channel nodeChannel = node2ChannelMap.get(nodeId);
				ReadFile replication = new ReadFile(workmessage, nodeChannel);
				Thread replicationThread = new Thread(replication);
				replicationThread.start();
			}

		}
	}

	public void readFileDB(WorkMessage workmessage) {
		// String fileName = workmessage.getFileName();
		DatabaseHandler dbhandler = new DatabaseHandler();
		// int hash = dbhandler.getFile(fileName);
		// return hash;

	}

	private class ReadFile implements Runnable {
		private WorkMessage workmessage;
		private Channel nodeChannel;

		public ReadFile(WorkMessage workmessage, Channel nodeChannel) {
			this.workmessage = workmessage;
			this.nodeChannel = nodeChannel;
		}

		@Override
		public void run() {
			if (this.nodeChannel.isOpen() && this.nodeChannel.isActive()) {
				// this.nodeChannel.writeAndFlush(workmessage);

				ChannelFuture cf = this.nodeChannel.write(workmessage);
				this.nodeChannel.flush();
				cf.awaitUninterruptibly();
				if (cf.isDone() && !cf.isSuccess()) {
					logger.error("Failed to send replication message to server");
				}

			} else {
				logger.error("The nodeChannel to " + nodeChannel.localAddress() + " is not active");
			}
		}
	}
}
