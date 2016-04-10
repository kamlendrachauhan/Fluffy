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
package gash.router.server.application;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.core.util.Constants;

import gash.monitor.client.MonitorClient;
import gash.router.cluster.GlobalEdgeMonitor;
import gash.router.cluster.GlobalInit;
import gash.router.cluster.GlobalServerState;
import gash.router.container.GlobalRoutingConf;
import gash.router.container.RoutingConf;
import gash.router.persistence.replication.DataReplicationManager;
import gash.router.raft.leaderelection.ElectionManagement;
import gash.router.server.CommandInit;
import gash.router.server.NodeChannelManager;
import gash.router.server.QueueManager;
import gash.router.server.ServerState;
import gash.router.server.WorkInit;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.tasks.NoOpBalancer;
import gash.router.server.tasks.TaskList;
import gash.server.util.MessageBuilder;
import gash.server.util.MessageGeneratorUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import pipe.monitor.Monitor.ClusterMonitor;

public class MessageServer {
	protected static Logger logger = LoggerFactory.getLogger("server");
	private static long tick = 0;
	private String monitorHost;
	private int monitorPort;

	protected static HashMap<Integer, ServerBootstrap> bootstrap = new HashMap<Integer, ServerBootstrap>();

	// public static final String sPort = "port";
	// public static final String sPoolSize = "pool.size";

	protected RoutingConf conf;
	protected GlobalRoutingConf globalConf;

	protected boolean background = true;
	public static int nodeId;

	public static int processed = 0;
	public static int stolen = 0;
	private boolean isMonitering = false;
	// State of the node (Leader/Non-leader, LeaderIP and LeaderID)

	/**
	 * initialize the server with a configuration of it's resources
	 * 
	 * @param cfg
	 */
	public MessageServer(File cfg, File globalCfg, boolean isMonitering) {
		init(cfg);
		initGlobalConfig(globalCfg);
		this.isMonitering = isMonitering;
	}

	public MessageServer(RoutingConf conf, GlobalRoutingConf globalConf) {
		this.conf = conf;
		this.globalConf = globalConf;
	}

	public void release() {
	}

	public void startServer() {

		QueueManager.initManager();
		DataReplicationManager.initDataReplicationManager();
		MessageGeneratorUtil.initGenerator();
		MessageGeneratorUtil.setRoutingConf(conf, globalConf);
		MessageBuilder.setRoutingConf(conf);

		StartGlobalCommunication globalComm = new StartGlobalCommunication(this.globalConf);
		logger.info("Starting global communication");
		Thread gThread = new Thread(globalComm);
		gThread.start();

		StartWorkCommunication comm = new StartWorkCommunication(conf);
		logger.info("Work starting");

		// We always start the worker in the background
		Thread cthread = new Thread(comm);
		cthread.start();

		if (!conf.isInternalNode()) {

			StartCommandCommunication comm2 = new StartCommandCommunication(conf);
			logger.info("Command starting");

			if (background) {
				Thread cthread2 = new Thread(comm2);
				cthread2.start();
			} else
				comm2.run();
		}

		// Starting the node manager
		NodeChannelManager.initNodeChannelManager();

		// Starting the monitor poller
		if (isMonitering) {
			MonitorClient client = MonitorClient.initConnection(gash.server.util.Constants.MONITOR_HOST,
					gash.server.util.Constants.MONITRO_PORT);
			MonitorScheduler ms = new MonitorScheduler(client);
			Timer timer = new Timer(false);
			timer.schedule(ms, 1000, 1000);
		}
		// Check for leader election to happen
		if (NodeChannelManager.amIPartOfNetwork) {
			while (!ElectionManagement.isReadyForElection())
				;
			ElectionManagement electionManagement = ElectionManagement.initElectionManagement(conf, globalConf);
			ElectionManagement.startElection();
		}

	}

	/**
	 * static because we need to get a handle to the factory from the shutdown
	 * resource
	 */
	public static void shutdown() {
		logger.info("Server shutdown");
		System.exit(0);
	}

	private void init(File cfg) {
		if (!cfg.exists())
			throw new RuntimeException(cfg.getAbsolutePath() + " not found");
		// resource initialization - how message are processed
		BufferedInputStream br = null;
		try {
			byte[] raw = new byte[(int) cfg.length()];
			br = new BufferedInputStream(new FileInputStream(cfg));
			br.read(raw);
			conf = JsonUtil.decode(new String(raw), RoutingConf.class);
			if (!verifyConf(conf))
				throw new RuntimeException("verification of configuration failed");
			nodeId = conf.getNodeId();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void initGlobalConfig(File globalCfg) {
		if (!globalCfg.exists())
			throw new RuntimeException(globalCfg.getAbsolutePath() + " not found");
		// resource initialization - how message are processed
		BufferedInputStream br = null;
		try {
			byte[] raw = new byte[(int) globalCfg.length()];
			br = new BufferedInputStream(new FileInputStream(globalCfg));
			br.read(raw);
			this.globalConf = JsonUtil.decode(new String(raw), GlobalRoutingConf.class);
			if (!verifyGlobalConf(this.globalConf))
				throw new RuntimeException("verification of configuration failed");
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private boolean verifyConf(RoutingConf conf) {
		return (conf != null);
	}

	private boolean verifyGlobalConf(GlobalRoutingConf conf) {
		return (conf != null);
	}

	public static int getNodeId() {
		return nodeId;
	}

	/**
	 * initialize netty communication
	 * 
	 * @param port
	 *            The port to listen to
	 */
	private static class StartCommandCommunication implements Runnable {
		RoutingConf conf;

		public StartCommandCommunication(RoutingConf conf) {
			this.conf = conf;
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				ServerBootstrap b = new ServerBootstrap();
				bootstrap.put(conf.getCommandPort(), b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

				boolean compressComm = false;
				b.childHandler(new CommandInit(conf, compressComm));

				// Start the server.
				logger.info("Starting command server (" + conf.getNodeId() + "), listening on port = "
						+ conf.getCommandPort());
				ChannelFuture f = b.bind(conf.getCommandPort()).syncUninterruptibly();

				logger.info(f.channel().localAddress() + " -> open: " + f.channel().isOpen() + ", write: "
						+ f.channel().isWritable() + ", act: " + f.channel().isActive());

				// block until the server socket is closed.
				f.channel().closeFuture().sync();

			} catch (Exception ex) {
				// on bind().sync()
				logger.error("Failed to setup handler.", ex);
			} finally {
				// Shut down all event loops to terminate all threads.
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();
			}
		}
	}

	/**
	 * initialize netty communication
	 * 
	 * @param port
	 *            The port to listen to
	 */
	private static class StartWorkCommunication implements Runnable {
		ServerState state;

		public StartWorkCommunication(RoutingConf conf) {
			if (conf == null)
				throw new RuntimeException("missing conf");

			state = new ServerState();
			state.setConf(conf);

			TaskList tasks = new TaskList(new NoOpBalancer());
			state.setTasks(tasks);

			EdgeMonitor emon = new EdgeMonitor(state);
			Thread t = new Thread(emon);
			t.start();
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				ServerBootstrap b = new ServerBootstrap();
				bootstrap.put(state.getConf().getWorkPort(), b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

				boolean compressComm = false;
				b.childHandler(new WorkInit(state, compressComm));

				// Start the server.
				logger.info("Starting work server (" + state.getConf().getNodeId() + "), listening on port = "
						+ state.getConf().getWorkPort());
				ChannelFuture f = b.bind(state.getConf().getWorkPort()).syncUninterruptibly();

				logger.info(f.channel().localAddress() + " -> open: " + f.channel().isOpen() + ", write: "
						+ f.channel().isWritable() + ", act: " + f.channel().isActive());

				// block until the server socket is closed.
				f.channel().closeFuture().sync();

			} catch (Exception ex) {
				// on bind().sync()
				logger.error("Failed to setup handler.", ex);
			} finally {
				// Shut down all event loops to terminate all threads.
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();

				// shutdown monitor
				EdgeMonitor emon = state.getEmon();
				if (emon != null)
					emon.shutdown();
			}
		}
	}

	/**
	 * initialize netty communication
	 * 
	 * @param port
	 *            The port to listen to
	 */
	private static class StartGlobalCommunication implements Runnable {
		GlobalRoutingConf globalRoutingConfig;
		GlobalServerState state;

		public StartGlobalCommunication(GlobalRoutingConf conf) {
			if (conf == null)
				throw new RuntimeException("missing global conf");
			this.globalRoutingConfig = conf;
			state = new GlobalServerState();
			state.setConf(conf);

			GlobalEdgeMonitor gemon = new GlobalEdgeMonitor(state);
			Thread t = new Thread(gemon);
			t.start();
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				ServerBootstrap b = new ServerBootstrap();
				System.out.println("Global Command Host & Port ::: " + globalRoutingConfig.getGlobalCommandHost()
						+ globalRoutingConfig.getGlobalCommandPort());

				bootstrap.put(globalRoutingConfig.getGlobalCommandPort(), b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

				boolean compressComm = false;
				b.childHandler(new GlobalInit(globalRoutingConfig, compressComm));

				// Start the server.
				logger.info("Starting Global Command server (" + globalRoutingConfig.getNodeId()
						+ "), listening on port = " + globalRoutingConfig.getGlobalCommandPort());
				ChannelFuture f = b.bind(globalRoutingConfig.getGlobalCommandPort()).syncUninterruptibly();

				logger.info(f.channel().localAddress() + " -> open: " + f.channel().isOpen() + ", write: "
						+ f.channel().isWritable() + ", act: " + f.channel().isActive());

				// block until the server socket is closed.
				f.channel().closeFuture().sync();

			} catch (Exception ex) {
				// on bind().sync()
				logger.error("Failed to setup handler.", ex);
			} finally {
				// Shut down all event loops to terminate all threads.
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();

				// shutdown monitor
				GlobalEdgeMonitor emon = state.getEmon();
				if (emon != null)
					emon.shutdown();
			}
		}
	}

	/**
	 * help with processing the configuration information
	 * 
	 * @author gash
	 *
	 */
	public static class JsonUtil {
		private static JsonUtil instance;

		public static void init(File cfg) {

		}

		public static JsonUtil getInstance() {
			if (instance == null)
				throw new RuntimeException("Server has not been initialized");

			return instance;
		}

		public static String encode(Object data) {
			try {
				ObjectMapper mapper = new ObjectMapper();
				return mapper.writeValueAsString(data);
			} catch (Exception ex) {
				return null;
			}
		}

		public static <T> T decode(String data, Class<T> theClass) {
			try {
				ObjectMapper mapper = new ObjectMapper();
				return mapper.readValue(data.getBytes(), theClass);
			} catch (Exception ex) {
				ex.printStackTrace();
				return null;
			}
		}
	}

	public class MonitorScheduler extends TimerTask {

		MonitorClient ma;

		public MonitorScheduler(MonitorClient client) {
			this.ma = client;
		}

		@Override
		public void run() {
			try {
				ClusterMonitor clusterMonitor = MessageGeneratorUtil.getInstance().generateNodeStatusMessage(tick++);
				ma.write(clusterMonitor);
			} catch (Exception e) {
				logger.error("Exception has occured " + e);
			}

		}

	}

}
