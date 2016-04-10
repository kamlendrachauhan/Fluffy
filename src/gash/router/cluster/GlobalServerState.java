package gash.router.cluster;

import gash.router.container.GlobalRoutingConf;

public class GlobalServerState {
	
	private GlobalRoutingConf conf;
	private GlobalEdgeMonitor emon;

	public GlobalRoutingConf getConf() {
		return conf;
	}

	public void setConf(GlobalRoutingConf conf) {
		this.conf = conf;
	}

	public GlobalEdgeMonitor getEmon() {
		return emon;
	}

	public void setEmon(GlobalEdgeMonitor emon) {
		this.emon = emon;
	}

}
