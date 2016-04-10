package gash.server.util;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.container.GlobalRoutingConf;
import gash.router.container.RoutingConf;
import gash.router.persistence.MessageDetails;
import gash.router.server.NodeChannelManager;
import gash.router.server.QueueManager;
import gash.router.server.application.MessageServer;
import global.Global.GlobalCommandMessage;
import pipe.common.Common.Failure;
import pipe.common.Common.Header;
import pipe.common.Common.Task;
import pipe.common.Common.Task.TaskType;
import pipe.monitor.Monitor.ClusterMonitor;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.StateOfLeader;
import pipe.work.Work.WorkMessage.Worktype;
import pipe.work.Work.WorkSteal;
import routing.Pipe.CommandMessage;
import storage.Storage.Action;
import storage.Storage.Query;
import storage.Storage.Response;

public class MessageGeneratorUtil {

	private static RoutingConf conf;
	private static GlobalRoutingConf globalRoutingConf;

	protected static Logger logger = LoggerFactory.getLogger(MessageGeneratorUtil.class);
	protected static AtomicReference<MessageGeneratorUtil> instance = new AtomicReference<MessageGeneratorUtil>();

	public static MessageGeneratorUtil initGenerator() {
		instance.compareAndSet(null, new MessageGeneratorUtil());
		return instance.get();
	}

	private MessageGeneratorUtil() {

	}

	public static MessageGeneratorUtil getInstance(){
		MessageGeneratorUtil mgu = instance.get();
		if(mgu == null){
			logger.error(" Error while getting instance of MessageGeneratorUtil ");
		}
		return mgu;
	}

	/**
	 * Leader sends this message to each node to replicate the data in the CommandMessage.
	 * @param message
	 * @param nodeId
	 * @return
	 */
	public WorkMessage generateReplicationReqMsg(CommandMessage message, Integer nodeId){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);

		Task.Builder tb = Task.newBuilder(message.getTask());
		tb.setChunk(message.getTask().getChunk());
		wb.setTask(tb);
		//TODO Generate secret
		wb.setSecret(1234);
		wb.setIsProcessed(false);
		wb.setWorktype(Worktype.LEADER_WRITE);
		addLeaderFieldToWorkMessage(wb);

		return wb.build();
	}

	/**
	 * Leader sends this to client after it has finished processing write request.
	 * @param isSuccess
	 * @param nodeId
	 * @return
	 */
	public CommandMessage generateClientResponseMsg(boolean isSuccess){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		if(isSuccess)
			rb.setMessage(" File saved ");
		else
			rb.setMessage(" Operation Failed ");

		return rb.build();

	}

	/**
	 * Leader sends this to a slave node to service a READ request. 
	 * @param commandMessage
	 * @return
	 */
	public WorkMessage generateDelegationMessage(CommandMessage commandMessage, String requestID){

		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setTask(commandMessage.getTask());
		wb.setIsProcessed(false);
		//TODO Set the secret
		wb.setSecret(1234);
		wb.setRequestId(requestID);
		wb.setWorktype(Worktype.LEADER_READ);
		addLeaderFieldToWorkMessage(wb);

		return wb.build();
	}
	
	public CommandMessage generateRiakFileMessage(byte[] fileBytes,String fileName){

		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(MessageServer.getNodeId());
		hb.setTime(System.currentTimeMillis());

		CommandMessage.Builder cb = CommandMessage.newBuilder();
		cb.setHeader(hb.build());
		
		Task.Builder tb = Task.newBuilder();
		tb.setChunkNo(1);
		tb.setNoOfChunks(1);
		tb.setChunk(ByteString.copyFrom(fileBytes));
		tb.setFilename(fileName);
		
		cb.setTask(tb);
		cb.setMessage("Success");
		return cb.build();
	}


	public WorkMessage generateStealMessage(){

		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkSteal.Builder stealMessage = WorkSteal.newBuilder();
		stealMessage.setStealtype(pipe.work.Work.WorkSteal.StealType.STEAL_REQUEST);

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setIsProcessed(false);
		//TODO Set the secret
		wb.setSecret(1234);
		addLeaderFieldToWorkMessage(wb);
		wb.setSteal(stealMessage);

		return wb.build();
	}


	/**
	 * This message is sent from the slave to the leader when leader has requested a READ. A msg is generated for each file chunk.
	 * @return
	 */
	public WorkMessage generateDelegationRespMsg(Task t, byte[] currentByte, int chunkId, int totalChunks, String requestId){
		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		//TODO Set the secret
		wb.setSecret(1234);
		wb.setRequestId(requestId);
		wb.setWorktype(Worktype.SLAVE_READ_DONE);

		Task.Builder tb = Task.newBuilder();
		tb.setChunkNo(chunkId);
		tb.setChunk(ByteString.copyFrom(currentByte));
		tb.setNoOfChunks(totalChunks);
		tb.setTaskType(TaskType.READ);
		tb.setFilename(t.getFilename());
		tb.setSender(t.getSender());
		if(chunkId == totalChunks){
			wb.setIsProcessed(true);
		}else{
			wb.setIsProcessed(false);
		}

		wb.setTask(tb.build());
		addLeaderFieldToWorkMessage(wb);

		return wb.build();
	}
	
	/**
	 * This message is sent from the slave to the leader when leader has requested a READ. A msg is generated for each file chunk.
	 * @return
	 */
	public WorkMessage generateStolenDelegationRespMsg(Task t, byte[] currentByte, int chunkId, int totalChunks, String requestId){
		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		//TODO Set the secret
		wb.setSecret(1234);
		wb.setRequestId(requestId);
		wb.setWorktype(Worktype.SLAVE_READ_DONE);
		wb.setIsStolen(true);

		Task.Builder tb = Task.newBuilder();
		tb.setChunkNo(chunkId);
		tb.setChunk(ByteString.copyFrom(currentByte));
		tb.setNoOfChunks(totalChunks);
		tb.setTaskType(TaskType.READ);
		tb.setFilename(t.getFilename());
		tb.setSender(t.getSender());
		if(chunkId == totalChunks){
			wb.setIsProcessed(true);
		}else{
			wb.setIsProcessed(false);
		}

		wb.setTask(tb.build());
		addLeaderFieldToWorkMessage(wb);

		return wb.build();
	}
	

	/**
	 * Generates a Command Message from the Work Message sent by the slave node and sends it to the client.
	 * @param message
	 * @return
	 */
	public CommandMessage forwardChunkToClient(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(MessageServer.getNodeId());
		hb.setTime(System.currentTimeMillis());

		CommandMessage.Builder cb = CommandMessage.newBuilder();
		cb.setHeader(hb.build());
		cb.setTask(message.getTask());
		cb.setMessage("Success");


		return cb.build();
	}

	/**
	 * Sent from a slave node to the leader after it has completed replication. It sets the tasktype flag to SLAVE_WRITTEN.
	 * @param message
	 * @return
	 */
	public WorkMessage generateReplicationAckMessage(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		/*Task.Builder tb = Task.newBuilder(message.getTask());
		tb.clearChunk();
		tb.clearChunkNo();
		tb.clearNoOfChunks();*/

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setWorktype(Worktype.SLAVE_WRITTEN);
		//TODO Set the secret
		wb.setSecret(1234);
		//wb.setTask(tb.build());

		addLeaderFieldToWorkMessage(wb);

		return wb.build();
	}


	public WorkMessage generateNewNodeReplicationMsg(MessageDetails details,String sender){
		logger.info("Generating new node replication message from node "+sender);
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());

		Task.Builder tb = Task.newBuilder();
		tb.setChunkNo(details.getChunckId());
		tb.setNoOfChunks(details.getNoOfChuncks());
		tb.setChunk(ByteString.copyFrom(details.getByteData()));
		tb.setTaskType(TaskType.WRITE);
		tb.setSender(sender);
		tb.setFilename(details.getFileName());
		wb.setTask(tb);
		//TODO Generate secret
		wb.setSecret(1234);
		wb.setIsProcessed(false);
		wb.setWorktype(Worktype.LEADER_WRITE);
		addLeaderFieldToWorkMessage(wb);

		return wb.build();
	}

	
	public ClusterMonitor generateNodeStatusMessage(long tickVal){
		ClusterMonitor.Builder cmb = ClusterMonitor.newBuilder();
		QueueManager queueManager = QueueManager.getInstance();
		cmb.setClusterId(420);
		cmb.addEnqueued(queueManager.getInboundCommQSize()+queueManager.getOutboundWorkQSize());
		cmb.addProcessed(MessageServer.processed);
		cmb.addStolen(MessageServer.stolen);
		cmb.setNumNodes(NodeChannelManager.node2ChannelMap.size()+1);
		cmb.addProcessId(0);
		cmb.setTick(tickVal);
		
		return cmb.build();
	}

	
	/**
	 * Generates a Heartbeat msg sent from leader to slave.
	 * @return
	 */
	public WorkMessage generateHeartbeat(){
		return null;
	}

	/**
	 * Generate Heartbeat response sent from the slave to the leader. Includes the system CPU utilization.
	 * @return
	 */
	public WorkMessage generateHeartbeatResponse(){
		return null;
	}
	
	/**
	 * Global Message construction
	 */
	public GlobalCommandMessage generateReadRequestGlobalCommmandMessage(String fileName, int nodeID) {
		GlobalCommandMessage.Builder builder = GlobalCommandMessage.newBuilder();
		Query.Builder queryBuilder = Query.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();

		headerBuilder.setNodeId(nodeID);
		headerBuilder.setTime(System.currentTimeMillis());

		queryBuilder.setAction(Action.GET);
		queryBuilder.setKey(fileName);

		builder.setHeader(headerBuilder);
		builder.setQuery(queryBuilder);

		return builder.build();
	}

	public WorkMessage convertGlobalCommandMessage2WorkMessage(GlobalCommandMessage globalCommandMessage) {

		Response globalMessageResponse = globalCommandMessage.getResponse();

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(MessageServer.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		// TODO Set the secret
		wb.setSecret(1234);
		wb.setRequestId(globalMessageResponse.getKey());
		wb.setWorktype(Worktype.SLAVE_READ_DONE);

		Task.Builder tb = Task.newBuilder();
		tb.setChunkNo(globalMessageResponse.getSequenceNo());
		tb.setChunk(globalMessageResponse.getData());
		tb.setNoOfChunks(globalMessageResponse.getMetaData().getSeqSize());
		tb.setTaskType(TaskType.READ);
		tb.setFilename(globalMessageResponse.getKey());
		tb.setSender(String.valueOf(globalCommandMessage.getHeader().getNodeId()));
		if (globalMessageResponse.getSequenceNo() == globalMessageResponse.getMetaData().getSeqSize()) {
			wb.setIsProcessed(true);
		} else {
			wb.setIsProcessed(false);
		}

		wb.setTask(tb.build());
		addLeaderFieldToWorkMessage(wb);
		return wb.build();
	}

	public CommandMessage convertGlobalCommandMessage2CommandMessage(GlobalCommandMessage globalCommandMessage) {
		CommandMessage.Builder commandBuilder = CommandMessage.newBuilder();

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(MessageServer.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);

		Task.Builder tb = Task.newBuilder();
		tb.setChunk(globalCommandMessage.getQuery().getData());
		wb.setTask(tb);

		wb.setSecret(1234);
		wb.setIsProcessed(false);
		wb.setWorktype(Worktype.LEADER_WRITE);
		addLeaderFieldToWorkMessage(wb);
		return commandBuilder.build();
	}

	public GlobalCommandMessage generateReadResonseGlobalCommmandMessage(String fileName, byte[] currentByte,
			int chunkId, int totalChunks) {
		GlobalCommandMessage.Builder builder = GlobalCommandMessage.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		Response.Builder responseBuilder = Response.newBuilder();

		headerBuilder.setNodeId(MessageServer.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());

		responseBuilder.setAction(Action.GET);
		responseBuilder.setKey(fileName);
		responseBuilder.setData(ByteString.copyFrom(currentByte));
		responseBuilder.setSuccess(true);
		responseBuilder.setSequenceNo(chunkId);

		builder.setHeader(headerBuilder);
		builder.setResponse(responseBuilder);

		return builder.build();
	}

	public GlobalCommandMessage generateReadFailResponseGlobalCommmandMessage(String fileName) {
		GlobalCommandMessage.Builder builder = GlobalCommandMessage.newBuilder();

		Response.Builder responseBuilder = Response.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		Failure.Builder eb = Failure.newBuilder();

		eb.setId(conf.getNodeId());
		eb.setRefId(conf.getNodeId());
		eb.setMessage("The requested file is not available on our cluster");

		headerBuilder.setNodeId(conf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());

		responseBuilder.setAction(Action.GET);
		responseBuilder.setKey(fileName);
		responseBuilder.setSuccess(false);
		responseBuilder.setFailure(eb);

		builder.setHeader(headerBuilder);
		builder.setResponse(responseBuilder);

		return builder.build();
	}

	public GlobalCommandMessage generateWriteResponseToGlobalCommandAdapter(GlobalCommandMessage globalCommandMessage,
			boolean isSuccess) {
		GlobalCommandMessage.Builder builder = GlobalCommandMessage.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		Response.Builder responseBuilder = Response.newBuilder();

		headerBuilder.setNodeId(MessageServer.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());

		responseBuilder.setAction(Action.STORE);
		responseBuilder.setKey(globalCommandMessage.getQuery().getKey());
		responseBuilder.setSuccess(isSuccess);
		responseBuilder.setSequenceNo(globalCommandMessage.getQuery().getSequenceNo());

		builder.setHeader(headerBuilder);
		builder.setResponse(responseBuilder);

		return builder.build();

	}

	private void addLeaderFieldToWorkMessage(WorkMessage.Builder wb) {
		if (NodeChannelManager.currentLeaderID == 0) {
			wb.setStateOfLeader(StateOfLeader.LEADERUNKNOWN);
		} else if (NodeChannelManager.currentLeaderID == conf.getNodeId()) {
			// Current Node is the leader
			wb.setStateOfLeader(StateOfLeader.LEADERALIVE);
		} else {
			wb.setStateOfLeader(StateOfLeader.LEADERKNOWN);
		}
	}

	public static void setRoutingConf(RoutingConf routingConf, GlobalRoutingConf globalRoutingConf) {
		MessageGeneratorUtil.conf = routingConf;
		MessageGeneratorUtil.globalRoutingConf = globalRoutingConf;
	}
}
