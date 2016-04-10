package gash.router.server.model;

import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

/**
 * Object which is stored in the Master's queue. Channel is stored so we can
 * directly send a response to the client on this channel. It only handles
 * Work messages.
 * 
 * @author savio
 *
 */
public class WorkMessageChannelCombo {
	private Channel channel;
	private WorkMessage workMessage;

	public WorkMessageChannelCombo(Channel channel, WorkMessage workMessage) {
		super();
		this.channel = channel;
		this.workMessage = workMessage;
	}

	public Channel getChannel() {
		return channel;
	}

	public WorkMessage getWorkMessage() {
		return workMessage;
	}

}