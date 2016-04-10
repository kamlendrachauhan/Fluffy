package gash.router.raft.leaderelection;

public class ElectionTImer implements Runnable {

	@Override
	public void run() {
		ElectionManagement.resetElection();
		ElectionManagement.startElection();
	}

}
