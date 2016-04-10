package gash.router.cluster;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeList;
import global.Global.GlobalCommandMessage;
import io.netty.channel.Channel;

public class ClusterNodeChannelManager {
	protected static Logger logger = LoggerFactory.getLogger(ClusterNodeChannelManager.class);

	public static void broadcastGlobalCommandMessage(GlobalCommandMessage globalCommandMessage) {
		EdgeList outboundEdges = GlobalEdgeMonitor.getOutboundEdges();
		if (outboundEdges.map.isEmpty()) {
			System.out.println("----- No nodes are availble -----");
			return;
		}

		Collection<EdgeInfo> collectionOfEdgeInfo = outboundEdges.map.values();
		for (EdgeInfo globalEdgeInfo : collectionOfEdgeInfo) {
			Channel channel = globalEdgeInfo.getChannel();
			if (channel.isActive() && channel.isWritable()) {
				logger.debug("Sending message to Channel : " + channel.toString());
				channel.writeAndFlush(globalCommandMessage);
			} else {
				logger.error("Error occured while broacasting message to " + channel.toString()
						+ " :: Channel not writable or not active");
			}
		}
	}
}
