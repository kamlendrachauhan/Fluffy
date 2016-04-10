package gash.router.persistence.replication;

public class ReplicationInfo {

	private String fileName;
	private byte[] fileContent;
	private int chunkOrder;
	private int totalChunkCount;
	private int nodeId;

	public ReplicationInfo() {

	}
	
	public ReplicationInfo(String fileName, byte[] fileContent, int chunkOrder, int totalChunkCount) {
		super();
		this.fileName = fileName;
		this.fileContent = fileContent;
		this.chunkOrder = chunkOrder;
		this.totalChunkCount = totalChunkCount;
	}




	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public byte[] getFileContent() {
		return fileContent;
	}

	public void setFileContent(byte[] fileContent) {
		this.fileContent = fileContent;
	}

	public int getChunkOrder() {
		return chunkOrder;
	}

	public void setChunkOrder(int chunkOrder) {
		this.chunkOrder = chunkOrder;
	}
	
	public int getTotalChunkCount() {
		return totalChunkCount;
	}
	
	public void setTotalChunkCount(int totalChunkCount) {
		this.totalChunkCount = totalChunkCount;
	}
	
	public int getNodeId() {
		return nodeId;
	}
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

}
