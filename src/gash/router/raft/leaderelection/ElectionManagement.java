package gash.router.raft.leaderelection;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.GlobalRoutingConf;
import gash.router.container.RoutingConf;
import gash.router.server.NodeChannelManager;
import gash.router.server.ServerState;
import gash.server.util.Constants;
import gash.server.util.MessageBuilder;
import gash.server.util.RandomTimeoutGenerator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.election.Election.RaftMessage;
import pipe.work.Work.WorkMessage;

public class ElectionManagement {
	protected static Logger logger = LoggerFactory.getLogger("ElectionManagement");

	protected static AtomicReference<ElectionManagement> instance = new AtomicReference<ElectionManagement>();

	private static RoutingConf routingConf = null;
	private static GlobalRoutingConf globalRoutingConf = null;
	private static RaftNodeStateMachine raftStateMachine;
	private static Timer electionTimer;
	private static ConcurrentHashMap<Integer, Boolean> voteCheckMap;
	private static int currentVoteCount = 1;
	public static Channel globalChannel;
	private static long currentTimeoutTime = 0;
	private static ConcurrentHashMap<Integer, Boolean> voteCastedInCurrentTerm;
	private static int currentTerm = 0;

	public ElectionManagement() {

	}

	public static ElectionManagement getInstance() {
		if (instance.get() == null) {
			instance.compareAndSet(null, new ElectionManagement());
		}
		return instance.get();
	}

	public static ElectionManagement initElectionManagement(RoutingConf routingConf,
			GlobalRoutingConf globalRoutingConf) {
		ElectionManagement.routingConf = routingConf;
		ElectionManagement.raftStateMachine = new RaftNodeStateMachine();
		ElectionManagement.globalRoutingConf = globalRoutingConf;
		ElectionManagement.electionTimer = new Timer();
		ElectionManagement.voteCastedInCurrentTerm = new ConcurrentHashMap<Integer, Boolean>();
		ElectionManagement.voteCastedInCurrentTerm.put(currentTerm, false);
		ElectionManagement.voteCheckMap = new ConcurrentHashMap<Integer, Boolean>();
		MessageBuilder.setRoutingConf(routingConf);
		instance.compareAndSet(null, new ElectionManagement());
		return instance.get();
	}

	public static boolean isReadyForElection() {
		return NodeChannelManager.numberOfActiveChannels() >= Constants.MINIMUM_NUMBER_OF_NODES_REQUIRED ? true : false;
	}

	public static void startElection() {
		startElectionTimer();
	}

	private static void sendMessage() throws Exception {
		if (raftStateMachine.isCandidate()) {
			logger.debug("------ Building the election Message -----");
			currentTimeoutTime = System.currentTimeMillis();
			WorkMessage electionMessage = MessageBuilder.buildElectionMessage(currentTimeoutTime, currentTerm);
			logger.debug("Election Message " + electionMessage.toString());
			NodeChannelManager.broadcast(electionMessage);
		}
	}

	// To process incoming election work messages from other nodes
	public static synchronized void processElectionMessage(Channel incomingChannel, WorkMessage electionWorkMessage) {
		RaftMessage raftMessage = electionWorkMessage.getRaftMessage();

		switch (raftMessage.getAction()) {
		case REQUESTVOTE:
			// Check if the current term is less than the new term i.e. new
			// election has begin
			if (currentTerm < raftMessage.getTerm()) {
				currentTerm = raftMessage.getTerm();
				if (voteCastedInCurrentTerm != null)
					ElectionManagement.voteCastedInCurrentTerm.put(currentTerm, false);
				else {
					ElectionManagement.voteCastedInCurrentTerm = new ConcurrentHashMap<Integer, Boolean>();
					ElectionManagement.voteCastedInCurrentTerm.put(currentTerm, false);
				}

			}

			switch (ElectionManagement.raftStateMachine.getState()) {
			case Follower:
				// If this node receives any Request Vote message and has not
				// yet voted in current term
				if (!voteCastedInCurrentTerm.get(currentTerm)) {
					WorkMessage electionResponseMessage = MessageBuilder.buildElectionResponseMessage(
							electionWorkMessage.getRaftMessage().getRequestVote().getCandidateId(), true,
							electionWorkMessage.getRaftMessage().getTerm());
					if (!voteCheckMap.containsKey(routingConf.getNodeId())) {
						voteCheckMap.put(routingConf.getNodeId(), true);
					}
					logger.debug("Vote Casted in favour of : "
							+ electionResponseMessage.getRaftMessage().getRequestVote().getCandidateId());
					ChannelFuture cf = incomingChannel.write(electionResponseMessage);
					incomingChannel.flush();

					cf.awaitUninterruptibly();
					voteCastedInCurrentTerm.put(currentTerm, true);
				} else {
					logger.debug("Already casted vote for this term, Not casting again");
				}
				break;
			case Candidate:
				/*
				 * Vote request has come to a candidate node. Various conditions
				 * are possible in this situation
				 */
				if (currentTimeoutTime < electionWorkMessage.getHeader().getTime()) {
					// Discard this vote request as the current candidate is the
					// real one to be considered for leader.

				} else {
					// The message from the sender candidate is correct and
					// should be considered. Stepping down to follower state
					// TODO check if we need to send out any response
					raftStateMachine.becomeFollower();
				}
				break;
			case Leader:
				logger.debug("The leader received the vote request");
				// Check if the term is the current one or new term

				if (currentTerm < raftMessage.getTerm()) {
					currentTerm = raftMessage.getTerm();
					raftStateMachine.becomeFollower();

					WorkMessage electionResponseMessage = MessageBuilder.buildElectionResponseMessage(
							electionWorkMessage.getRaftMessage().getRequestVote().getCandidateId(), true,
							electionWorkMessage.getRaftMessage().getTerm());
					if (!voteCheckMap.containsKey(routingConf.getNodeId())) {
						voteCheckMap.put(routingConf.getNodeId(), true);
					}
					logger.debug("Vote Casted in favour of : "
							+ electionResponseMessage.getRaftMessage().getRequestVote().getCandidateId());
					incomingChannel.writeAndFlush(electionResponseMessage);
					voteCastedInCurrentTerm.put(currentTerm, true);
				} else {
					logger.debug("The current leader is the correct Leader");
				}

				break;

			default:
				break;
			}// End of raftStateMachine Switch case

			break;
		case VOTE:
			// If the other node is casting vote in the favor of this node
			if (raftStateMachine.isCandidate()) {
				/*
				 * Count the votes, If there is majority, send out leader
				 * message to all. To check whether the this request vote is for
				 * current node or not
				 */
				if (electionWorkMessage.getRaftMessage().getRequestVote().getCandidateId() == routingConf.getNodeId()) {
					boolean amILeader = decideIfLeader(electionWorkMessage);
					if (amILeader) {
						try {
							currentTimeoutTime = System.currentTimeMillis();
							raftStateMachine.becomeLeader();
							System.out.println("Node " + routingConf.getNodeId() + " became leader");
							WorkMessage leaderResponseMessage = MessageBuilder
									.buildLeaderResponseMessage(routingConf.getNodeId(), currentTerm);
							NodeChannelManager.broadcast(leaderResponseMessage);
							// Current Node is the leader
							NodeChannelManager.currentLeaderID = routingConf.getNodeId();
							NodeChannelManager.currentLeaderAddress = leaderResponseMessage.getRaftMessage()
									.getLeaderHost();

							/*
							 * Once the leader is up, leader should be connected
							 * to the Global Command Message Adapter Running on
							 * one of the hosts in the cluster
							 */
							ServerState serverState = new ServerState();
							serverState.setConf(routingConf);
							Channel globalAdaptersChannel = NodeChannelManager.getChannelByHostAndPort(serverState,
									globalRoutingConf);
							if (globalAdaptersChannel != null && globalAdaptersChannel.isActive()
									&& globalAdaptersChannel.isWritable()) {
								/*
								 * The channel created should be active and
								 * writable to be considered for communication
								 */
								logger.info("Global Adapters Channel " + globalAdaptersChannel);
								NodeChannelManager.setGlobalCommandAdapterChannel(globalRoutingConf,
										globalAdaptersChannel);
								globalChannel = globalAdaptersChannel;
							} else {
								logger.error("The global channel could not be created" + globalAdaptersChannel);
							}

						} catch (Exception exception) {
							System.out.println("An error has occured while broadcasting the message.");
						}
					} else {
						logger.debug("Still needs votes to win");
					}
				}

			}
			break;
		case LEADER:
			try {
				logger.debug("Current Leader ID : " + electionWorkMessage.getRaftMessage().getLeaderId());
				// Saving current leader id in NodeChannelManager to use it
				// across the node
				NodeChannelManager.currentLeaderID = electionWorkMessage.getRaftMessage().getLeaderId();
				NodeChannelManager.currentLeaderAddress = electionWorkMessage.getRaftMessage().getLeaderHost();

			} catch (Exception exception) {
				logger.error("Something is not correct", exception);
			}

			break;
		default:
			break;
		}
	}

	private static boolean decideIfLeader(WorkMessage electionWorkMessage) {
		// logic to count the number of votes received so far and
		// decide the leader
		if (electionWorkMessage.getRaftMessage().getRequestVote().getVoteGranted()) {

			currentVoteCount++;
			if (currentVoteCount >= ((NodeChannelManager.getNode2ChannelMap().size() / 2 + 1)))
				return true;
			else
				return false;

			// return true;
		}
		return false;
	}

	public static void becomeFollower() {
		ElectionManagement.raftStateMachine.becomeFollower();
	}

	public static void resetElection() {
		// Reset the State of the nodes and cluster
		raftStateMachine.becomeFollower();
		NodeChannelManager.currentLeaderID = 0;
		voteCheckMap = new ConcurrentHashMap<Integer, Boolean>();
		currentVoteCount = 0;
		ElectionManagement.initElectionManagement(routingConf, globalRoutingConf);
	}

	private static void startElectionTimer() {
		// Election timer
		int currentTimeout = RandomTimeoutGenerator.randTimeout() * routingConf.getNodeId();
		logger.debug("The Election Timeout for this node is : " + currentTimeout);
		electionTimer.schedule(new ElectionTimer(), (long) currentTimeout, (long) currentTimeout);
	}

	private static class ElectionTimer extends TimerTask {
		@Override
		public void run() {
			logger.debug("Timer Expired for " + routingConf.getNodeId());

			// Cancel the ongoing scheduled timer call electionTimer.cancel();
			if (voteCheckMap.containsKey(routingConf.getNodeId()) && voteCheckMap.get(routingConf.getNodeId())) {
				electionTimer.cancel();
				voteCheckMap.put(routingConf.getNodeId(), false);
			}
			// check if we haven't received the leader message from anybody else
			// if not set it as candidate
			if (raftStateMachine.isFollower() && !voteCheckMap.containsKey(routingConf.getNodeId())) {
				raftStateMachine.becomeCandidate();
				try {
					currentTerm++;

					ElectionManagement.sendMessage();
				} catch (Exception exception) {
					logger.error("An error has occured while sending message");
				}
			}

			if (raftStateMachine.isLeader()) {
				electionTimer.cancel();
			}
		}

	}

}
