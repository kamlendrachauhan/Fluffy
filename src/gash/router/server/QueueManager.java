package gash.router.server;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.cluster.InboundGlobalCommander;
import gash.router.cluster.OutboundGlobalCommander;
import gash.router.server.model.CommandMessageChannelCombo;
import gash.router.server.model.GlobalCommandMessageChannelCombo;
import gash.router.server.model.WorkMessageChannelCombo;
import global.Global.GlobalCommandMessage;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

/**
 * Handles the input queue for the server node. Polls the queue and runs tasks
 * from it whenever there is an element in the queue.
 * 
 * @author savio.fernandes
 * 
 */
public class QueueManager {

	protected static Logger logger = LoggerFactory.getLogger(QueueManager.class);
	protected static AtomicReference<QueueManager> instance = new AtomicReference<QueueManager>();

	protected LinkedBlockingDeque<CommandMessageChannelCombo> inboundCommQ;
	protected LinkedBlockingDeque<CommandMessageChannelCombo> outboundCommQ;
	protected InboundCommander inboundCommmander;
	protected OutboundCommander outboundCommmander;

	protected LinkedBlockingDeque<WorkMessageChannelCombo> inboundWorkQ;
	protected LinkedBlockingDeque<WorkMessageChannelCombo> outboundWorkQ;
	protected InboundWorker inboundWorker;
	protected OutboundWorker outboundWorker;

	protected LinkedBlockingDeque<GlobalCommandMessageChannelCombo> inboundGlobalCommandQ;
	protected LinkedBlockingDeque<GlobalCommandMessageChannelCombo> outboundGlobalCommandQ;
	protected InboundGlobalCommander inboundGlobalCommander;
	protected OutboundGlobalCommander outboundGlobalCommander;

	public static QueueManager initManager() {
		instance.compareAndSet(null, new QueueManager());
		return instance.get();
	}

	public static QueueManager getInstance() {
		if (instance == null)
			instance.compareAndSet(null, new QueueManager());
		return instance.get();
	}

	public LinkedBlockingDeque<WorkMessageChannelCombo> getInboundWorkQ() {
		return inboundWorkQ;
	}

	public QueueManager() {
		logger.info(" Started the Manager ");

		inboundCommQ = new LinkedBlockingDeque<CommandMessageChannelCombo>();
		outboundCommQ = new LinkedBlockingDeque<CommandMessageChannelCombo>();
		inboundCommmander = new InboundCommander(this);
		inboundCommmander.start();
		outboundCommmander = new OutboundCommander(this);
		outboundCommmander.start();

		inboundWorkQ = new LinkedBlockingDeque<WorkMessageChannelCombo>();
		outboundWorkQ = new LinkedBlockingDeque<WorkMessageChannelCombo>();
		inboundWorker = new InboundWorker(this);
		inboundWorker.start();
		outboundWorker = new OutboundWorker(this);
		outboundWorker.start();

		inboundGlobalCommandQ = new LinkedBlockingDeque<GlobalCommandMessageChannelCombo>();
		outboundGlobalCommandQ = new LinkedBlockingDeque<GlobalCommandMessageChannelCombo>();
		inboundGlobalCommander = new InboundGlobalCommander(this);
		inboundGlobalCommander.start();
		outboundGlobalCommander = new OutboundGlobalCommander(this);
		outboundGlobalCommander.start();
	}

	public LinkedBlockingDeque<GlobalCommandMessageChannelCombo> getInboundGlobalCommandQ() {
		return inboundGlobalCommandQ;
	}

	public void setInboundGlobalCommandQ(LinkedBlockingDeque<GlobalCommandMessageChannelCombo> inboundGlobalCommandQ) {
		this.inboundGlobalCommandQ = inboundGlobalCommandQ;
	}

	public LinkedBlockingDeque<GlobalCommandMessageChannelCombo> getOutboundGlobalCommandQ() {
		return outboundGlobalCommandQ;
	}

	public void setOutboundGlobalCommandQ(
			LinkedBlockingDeque<GlobalCommandMessageChannelCombo> outboundGlobalCommandQ) {
		this.outboundGlobalCommandQ = outboundGlobalCommandQ;
	}

	/*
	 * Functions for Command Messages
	 */
	public void enqueueInboundCommmand(CommandMessage message, Channel ch) {
		try {
			CommandMessageChannelCombo entry = new CommandMessageChannelCombo(ch, message);
			inboundCommQ.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	public CommandMessageChannelCombo dequeueInboundCommmand() throws InterruptedException {
		return inboundCommQ.take();
	}

	public void enqueueOutboundCommand(CommandMessage message, Channel ch) {
		try {
			CommandMessageChannelCombo entry = new CommandMessageChannelCombo(ch, message);
			outboundCommQ.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	public CommandMessageChannelCombo dequeueOutboundCommmand() throws InterruptedException {
		return outboundCommQ.take();
	}

	public void returnOutboundCommand(CommandMessageChannelCombo msg) throws InterruptedException {
		outboundCommQ.putFirst(msg);
	}

	public void returnInboundCommand(CommandMessageChannelCombo msg) throws InterruptedException {
		inboundCommQ.putFirst(msg);
	}

	/*
	 * End of Command Message methods
	 */

	/*
	 * Work Message methods
	 */

	public void enqueueInboundWork(WorkMessage message, Channel ch) {
		try {
			WorkMessageChannelCombo entry = new WorkMessageChannelCombo(ch, message);
			inboundWorkQ.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	public WorkMessageChannelCombo dequeueInboundWork() throws InterruptedException {
		return inboundWorkQ.take();
	}

	public void enqueueOutboundWork(WorkMessage message, Channel ch) {
		try {
			WorkMessageChannelCombo entry = new WorkMessageChannelCombo(ch, message);
			outboundWorkQ.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	public WorkMessageChannelCombo dequeueOutboundWork() throws InterruptedException {
		return outboundWorkQ.take();
	}

	public void returnOutboundWork(WorkMessageChannelCombo msg) throws InterruptedException {
		outboundWorkQ.putFirst(msg);
	}

	public void returnInboundWork(WorkMessageChannelCombo msg) throws InterruptedException {
		inboundWorkQ.putFirst(msg);
	}

	public int getInboundCommQSize() {
		return inboundCommQ.size();
	}

	public int getOutboundCommQSize() {
		return outboundCommQ.size();
	}

	public int getInboundWorkQSize() {
		return inboundWorkQ.size();
	}

	public int getOutboundWorkQSize() {
		return outboundWorkQ.size();
	}

	/*
	 * End of Work Message methods
	 */
	/*
	 * Functions for Global Command Messages
	 */
	public void enqueueGlobalInboundCommmand(GlobalCommandMessage message, Channel ch) {
		try {
			GlobalCommandMessageChannelCombo entry = new GlobalCommandMessageChannelCombo(ch, message);
			inboundGlobalCommandQ.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	public GlobalCommandMessageChannelCombo dequeueGlobalInboundCommmand() throws InterruptedException {
		return inboundGlobalCommandQ.take();
	}

	public void enqueueGlobalOutboundCommand(GlobalCommandMessage message, Channel ch) {
		try {
			GlobalCommandMessageChannelCombo entry = new GlobalCommandMessageChannelCombo(ch, message);
			outboundGlobalCommandQ.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	public GlobalCommandMessageChannelCombo dequeueGlobalOutboundCommmand() throws InterruptedException {
		return outboundGlobalCommandQ.take();
	}

	public void returnGlobalOutboundCommand(GlobalCommandMessageChannelCombo msg) throws InterruptedException {
		outboundGlobalCommandQ.putFirst(msg);
	}

	public void returnGlobalInboundCommand(GlobalCommandMessageChannelCombo msg) throws InterruptedException {
		inboundGlobalCommandQ.putFirst(msg);
	}

}
