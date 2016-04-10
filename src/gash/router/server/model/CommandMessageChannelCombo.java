package gash.router.server.model;

import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

/**
 * Object which is stored in the Master's queue. Channel is stored so we can
 * directly send a response to the client on this channel. It only handles
 * Command messages.
 * 
 * @author savio
 *
 */
public class CommandMessageChannelCombo {
	private Channel channel;
	private CommandMessage commandMessage;
	private int chunkCount = 0;

	public CommandMessageChannelCombo(Channel channel, CommandMessage commandMessage) {
		super();
		this.channel = channel;
		this.commandMessage = commandMessage;
	}

	public Channel getChannel() {
		return channel;
	}

	public CommandMessage getCommandMessage() {
		return commandMessage;
	}

	public void setChunkCount(int chunkCount) {
		this.chunkCount = chunkCount;
	}

	public int getChunkCount() {
		return chunkCount;
	}

	public synchronized void decrementChunkCount() {
		chunkCount--;
	}

}
