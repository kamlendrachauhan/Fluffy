package gash.router.server.model;

import global.Global.GlobalCommandMessage;
import io.netty.channel.Channel;

public class GlobalCommandMessageChannelCombo {
	private Channel channel;
	private GlobalCommandMessage globalCommandMessage;

	public GlobalCommandMessageChannelCombo(Channel channel, GlobalCommandMessage globalCommandMessage) {
		super();
		this.channel = channel;
		this.globalCommandMessage = globalCommandMessage;
	}

	public Channel getChannel() {
		return channel;
	}

	public GlobalCommandMessage getGlobalCommandMessage() {
		return globalCommandMessage;
	}

}
