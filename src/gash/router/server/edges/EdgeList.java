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
package gash.router.server.edges;

import java.util.concurrent.ConcurrentHashMap;

import gash.router.server.NodeChannelManager;
import io.netty.channel.Channel;

public class EdgeList {
	public ConcurrentHashMap<Integer, EdgeInfo> map = new ConcurrentHashMap<Integer, EdgeInfo>();

	public EdgeList() {
	}

	public EdgeInfo createIfNew(int ref, String host, int port) {
		if (hasNode(ref))
			return getNode(ref);
		else
			return addNode(ref, host, port);
	}

	public EdgeInfo createIfNew(int ref, String host, int port, Channel channel) {
		if (hasNode(ref))
			return getNode(ref);
		else {
			EdgeInfo edgeInfo = addNode(ref, host, port);
			edgeInfo.setChannel(channel);
			edgeInfo.setActive(true);
			return edgeInfo;
		}
	}

	public EdgeInfo addNode(int ref, String host, int port) {
		if (!verify(ref, host, port)) {
			// TODO log error
			throw new RuntimeException("Invalid node info");
		}

		if (!hasNode(ref)) {
			EdgeInfo ei = new EdgeInfo(ref, host, port);
			map.put(ref, ei);
			return ei;
		} else
			return null;
	}

	private boolean verify(int ref, String host, int port) {
		if (ref < 0 || host == null || port < 1024)
			return false;
		else
			return true;
	}

	public ConcurrentHashMap<Integer, EdgeInfo> getEdgeListMap() {
		return this.map;
	}

	public boolean hasNode(int ref) {
		return map.containsKey(ref);

	}

	public EdgeInfo getNode(int ref) {
		return map.get(ref);
	}

	public void removeNode(int ref) {
		map.remove(ref);
	}
	
	public void removeNodeByIp(String ip){
		for (Integer curr : map.keySet()) {
			if(map.get(curr).getHost().equals(ip)){
				map.get(curr).setChannel(null);
				NodeChannelManager.node2ChannelMap.remove(curr);
			}
		}
	}

	public void clear() {
		map.clear();
	}

	public boolean isEmpty() {
		if (map.size() > 0) {
			return false;
		}
		return true;
	}
}
