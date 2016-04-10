package gash.router.raft.leaderelection;

import java.util.concurrent.atomic.AtomicReference;

//State of the node
enum State {
	Leader, NonLeader;
}

public class NodeState {

	private static State nodeState;
	private static int leaderId;
	private static String leaderHostId;
	
	/**
	 * Added fields for maintaining the no. of processed tasks.
	 * @author savio
	 */
	private int processed =0;
	private int stolen = 0;

	protected static AtomicReference<NodeState> instance = new AtomicReference<NodeState>();

	public static NodeState init(){
		nodeState = State.NonLeader;
		instance.compareAndSet(null, new NodeState());
		return instance.get();
	}
	
	public static NodeState getInstance() {
		return instance.get();
	}

	public void setNodeState(State newState, int leaderId, String leaderHostId) {
		NodeState.nodeState = newState;
		NodeState.leaderId = leaderId;
		NodeState.leaderHostId = leaderHostId;
	}

	public void setNodeState(int leaderId, String leaderHostId) {
		NodeState.leaderId = leaderId;
		NodeState.leaderHostId = leaderHostId;
	}

	public State getNodeState() {
		return nodeState;
	}

	public void setNodeState(State nodeState) {
		NodeState.nodeState = nodeState;
	}

	public int getLeaderId() {
		return leaderId;
	}

	public void setLeaderId(int leaderId) {
		NodeState.leaderId = leaderId;
	}

	public String getLeaderHostId() {
		return leaderHostId;
	}

	public void setLeaderHostId(String leaderHostId) {
		NodeState.leaderHostId = leaderHostId;
	}
	
	public int getProcessed() {
		return processed;
	}
	
	public void incrementProcessed(){
		processed++;
	}
	
	public int getStolen() {
		return stolen;
	}
	public void incrementStolen(){
		stolen++;
	}
}