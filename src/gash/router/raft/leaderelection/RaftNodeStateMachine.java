package gash.router.raft.leaderelection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum ElectionState {
	Follower, Leader, Candidate;
}

public class RaftNodeStateMachine implements RaftStateMachine {

	static Logger logger = LoggerFactory.getLogger("StateMachine");

	private static ElectionState machineState;

	public RaftNodeStateMachine() {
		logger.info("Node became a follower");
		machineState = ElectionState.Follower;
	}


	@Override
	public void becomeFollower() {
		machineState = ElectionState.Follower;
	}

	@Override
	public void becomeCandidate() {
		logger.info("Node became a candidate");
		machineState = ElectionState.Candidate;
	}

	@Override
	public ElectionState getState() {
		return machineState;
	}

	@Override
	public boolean isLeader() {
		return machineState.equals(ElectionState.Leader);
	}

	@Override
	public void becomeLeader() {
		logger.info("Node became a Leader");
		machineState = ElectionState.Leader;
	}

	@Override
	public boolean isCandidate() {
		return machineState.equals(ElectionState.Candidate);
	}

	@Override
	public String toString() {
		return "Node is in following state" + machineState;
	}

	@Override
	public boolean isFollower() {
		return machineState.equals(ElectionState.Follower);
	}
}
