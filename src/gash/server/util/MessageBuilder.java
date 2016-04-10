package gash.server.util;

import gash.router.container.RoutingConf;
import gash.router.raft.leaderelection.HostAddressResolver;
import gash.router.server.NodeChannelManager;
import pipe.common.Common.Header;
import pipe.election.Election.LeaderStatus;
import pipe.election.Election.LeaderStatus.LeaderQuery;
import pipe.election.Election.LeaderStatus.LeaderState;
import pipe.election.Election.RaftMessage;
import pipe.election.Election.RaftMessage.ElectionAction;
import pipe.election.Election.VoteRequestMsg;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.StateOfLeader;

public class MessageBuilder {
	private static RoutingConf routingConf;

	public static WorkMessage buildElectionMessage(long electionTimeoutTime, int currentTerm) {
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		RaftMessage.Builder raftMessageBuilder = RaftMessage.newBuilder();
		VoteRequestMsg.Builder voteRequestMsgBuilder = VoteRequestMsg.newBuilder();

		headerBuilder.setElection(true);
		headerBuilder.setNodeId(routingConf.getNodeId());
		headerBuilder.setTime(electionTimeoutTime);

		voteRequestMsgBuilder.setCandidateId(routingConf.getNodeId());

		raftMessageBuilder.setAction(ElectionAction.REQUESTVOTE);
		raftMessageBuilder.setTerm(currentTerm);
		
		raftMessageBuilder.setRequestVote(voteRequestMsgBuilder);
		workMessageBuilder.setStateOfLeader(StateOfLeader.LEADERUNKNOWN);
		workMessageBuilder.setRaftMessage(raftMessageBuilder);
		workMessageBuilder.setSecret(1234);
		workMessageBuilder.setHeader(headerBuilder);
		return workMessageBuilder.build();
	}

	public static WorkMessage buildElectionResponseMessage(int candidateId, boolean response, int currentTerm) {
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		RaftMessage.Builder raftMessageBuilder = RaftMessage.newBuilder();
		VoteRequestMsg.Builder voteRequestMsgBuilder = VoteRequestMsg.newBuilder();

		headerBuilder.setElection(true);
		headerBuilder.setNodeId(routingConf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());

		voteRequestMsgBuilder.setCandidateId(candidateId);
		voteRequestMsgBuilder.setVoteGranted(response);

		raftMessageBuilder.setAction(ElectionAction.VOTE);
		raftMessageBuilder.setRequestVote(voteRequestMsgBuilder);
		raftMessageBuilder.setTerm(currentTerm);
		workMessageBuilder.setStateOfLeader(StateOfLeader.LEADERUNKNOWN);
		workMessageBuilder.setRaftMessage(raftMessageBuilder);
		workMessageBuilder.setSecret(1234);
		workMessageBuilder.setHeader(headerBuilder);
		return workMessageBuilder.build();
	}

	public static WorkMessage buildLeaderResponseMessage(int leaderId, int currentTerm) {
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		RaftMessage.Builder raftMessageBuilder = RaftMessage.newBuilder();

		headerBuilder.setElection(true);
		headerBuilder.setNodeId(routingConf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());

		raftMessageBuilder.setAction(ElectionAction.LEADER);
		raftMessageBuilder.setLeaderId(leaderId);
		raftMessageBuilder.setLeaderHost(HostAddressResolver.getLocalHostAddress());
		raftMessageBuilder.setTerm(currentTerm);
		
		workMessageBuilder.setStateOfLeader(StateOfLeader.LEADERALIVE);
		workMessageBuilder.setRaftMessage(raftMessageBuilder);
		workMessageBuilder.setHeader(headerBuilder);
		workMessageBuilder.setSecret(1234);

		return workMessageBuilder.build();
	}

	public static WorkMessage buildNewNodeLeaderStatusMessage() {
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		LeaderStatus.Builder leaderStatusBuilder = LeaderStatus.newBuilder();

		leaderStatusBuilder.setAction(LeaderQuery.WHOISTHELEADER);
		leaderStatusBuilder.setState(LeaderState.LEADERUNKNOWN);

		headerBuilder.setElection(false);
		headerBuilder.setNodeId(routingConf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());

		workMessageBuilder.setStateOfLeader(StateOfLeader.LEADERUNKNOWN);
		workMessageBuilder.setLeader(leaderStatusBuilder);
		workMessageBuilder.setHeader(headerBuilder);
		workMessageBuilder.setSecret(1234);

		return workMessageBuilder.build();
	}

	public static WorkMessage buildNewNodeLeaderStatusResponseMessage(int leaderId, String leaderHostAddress) {
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		LeaderStatus.Builder leaderStatusBuilder = LeaderStatus.newBuilder();

		leaderStatusBuilder.setAction(LeaderQuery.THELEADERIS);
		leaderStatusBuilder.setLeaderId(leaderId);
		leaderStatusBuilder.setLeaderHost(leaderHostAddress);

		headerBuilder.setElection(false);
		headerBuilder.setNodeId(routingConf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());

		if (routingConf.getNodeId() == NodeChannelManager.currentLeaderID)
			workMessageBuilder.setStateOfLeader(StateOfLeader.LEADERALIVE);
		else
			workMessageBuilder.setStateOfLeader(StateOfLeader.LEADERKNOWN);

		workMessageBuilder.setLeader(leaderStatusBuilder);
		workMessageBuilder.setHeader(headerBuilder);
		workMessageBuilder.setSecret(1234);

		return workMessageBuilder.build();
	}

	public static RoutingConf getRoutingConf() {
		return routingConf;
	}

	public static void setRoutingConf(RoutingConf routingConf) {
		MessageBuilder.routingConf = routingConf;
	}

}
