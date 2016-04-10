package gash.router.container;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "globalConf")
@XmlAccessorType(XmlAccessType.FIELD)
public class GlobalRoutingConf {
	private int nodeId;
	private int globalCommandPort;
	private int heartbeatDt = 2000;
	private String globalCommandHost;
	private List<RoutingEntry> routing;

	public HashMap<String, Integer> asHashMap() {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		if (routing != null) {
			for (RoutingEntry entry : routing) {
				map.put(entry.host, entry.port);
			}
		}
		return map;
	}

	public void addEntry(RoutingEntry entry) {
		if (entry == null)
			return;

		if (routing == null)
			routing = new ArrayList<RoutingEntry>();

		routing.add(entry);
	}

	public int getNodeId() {
		return nodeId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public int getGlobalCommandPort() {
		return globalCommandPort;
	}

	public void setGlobalCommandPort(int globalCommandPort) {
		this.globalCommandPort = globalCommandPort;
	}

	public int getHeartbeatDt() {
		return heartbeatDt;
	}

	public void setHeartbeatDt(int heartbeatDt) {
		this.heartbeatDt = heartbeatDt;
	}

	public List<RoutingEntry> getRouting() {
		return routing;
	}

	public void setRouting(List<RoutingEntry> conf) {
		this.routing = conf;
	}

	public String getGlobalCommandHost() {
		return globalCommandHost;
	}

	public void setGlobalCommandHost(String globalCommandHost) {
		this.globalCommandHost = globalCommandHost;
	}

	@XmlRootElement(name = "entry")
	@XmlAccessorType(XmlAccessType.PROPERTY)
	public static final class RoutingEntry {
		private String host;
		private int port;
		private int id;

		public RoutingEntry() {
		}

		public RoutingEntry(int id, String host, int port) {
			this.id = id;
			this.host = host;
			this.port = port;
		}

		public String getHost() {
			return host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

	}
}
