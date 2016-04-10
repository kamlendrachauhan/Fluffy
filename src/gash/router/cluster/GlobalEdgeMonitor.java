package gash.router.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.GlobalRoutingConf.RoutingEntry;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeList;
import gash.router.server.edges.EdgeListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class GlobalEdgeMonitor implements EdgeListener, Runnable {
	protected static Logger logger = LoggerFactory.getLogger(GlobalEdgeMonitor.class);

	private static EdgeList outboundEdges;
	private long dt = 2000;
	private GlobalServerState state;
	private boolean forever = true;

	public GlobalEdgeMonitor(GlobalServerState state) {
		if (state == null)
			throw new RuntimeException("state is null");

		outboundEdges = new EdgeList();
		this.state = state;
		this.state.setEmon(this);

		if (state.getConf().getRouting() != null) {
			for (RoutingEntry e : state.getConf().getRouting()) {
				outboundEdges.addNode(e.getId(), e.getHost(), e.getPort());
			}
		}

		// cannot go below 2 sec
		if (state.getConf().getHeartbeatDt() > this.dt)
			this.dt = state.getConf().getHeartbeatDt();

		System.out.println("-------------- Constructor GlobalEdgeMonitor -------------------");
	}

	public void createOutboundIfNew(int ref, String host, int port) {
		outboundEdges.createIfNew(ref, host, port);
	}

	/*
	 * private GlobalCommandMessage createGlobalCommandMessage(EdgeInfo ei) {
	 * GlobalCommandMessage.Builder sb = GlobalCommandMessage.newBuilder();
	 * 
	 * sb.setEnqueued(-1); sb.setProcessed(-1);
	 * 
	 * Heartbeat.Builder bb = Heartbeat.newBuilder(); bb.setState(sb);
	 * 
	 * Header.Builder hb = Header.newBuilder();
	 * hb.setNodeId(state.getConf().getNodeId()); hb.setDestination(-1);
	 * hb.setTime(System.currentTimeMillis());
	 * 
	 * WorkMessage.Builder wb = WorkMessage.newBuilder(); wb.setHeader(hb);
	 * wb.setBeat(bb); wb.setSecret(1234);
	 * 
	 * return sb.build(); }
	 */

	public void shutdown() {
		forever = false;
	}

	@Override
	public void run() {
		while (forever) {

			try {
				for (EdgeInfo ei : outboundEdges.map.values()) {
					try {
						if (ei.isActive() && ei.getChannel() != null) {
							// GlobalCommandMessage globalCommandMessage =
							// createGlobalCommandMessage(ei);
							// ei.getChannel().writeAndFlush(globalCommandMessage);

							logger.debug("Connected to Adapter : " + ei.getRef() + " : " + ei.getHost());

						} else if (ei.getChannel() == null) {
							Channel channel = connectToChannel(ei.getHost(), ei.getPort(), this.state);
							System.out.println("Channel " + channel);
							if (channel == null) {
								logger.debug("trying to connect to node " + ei.getRef());
							} else {
								ei.setChannel(channel);
								ei.setActive(channel.isActive());
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
				logger.error("Error has occured while connecting to global adapters", e);
			}
		}
	}

	private Channel connectToChannel(String host, int port, GlobalServerState state) {
		Bootstrap b = new Bootstrap();
		NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup();
		GlobalInit globalInit = new GlobalInit(state.getConf(), false);

		try {
			b.group(nioEventLoopGroup).channel(NioSocketChannel.class).handler(globalInit);
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

	@Override
	public synchronized void onAdd(EdgeInfo ei) {
		// TODO check connection
	}

	@Override
	public synchronized void onRemove(EdgeInfo ei) {
		// TODO ?
	}
}
