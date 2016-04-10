package gash.router.persistence;

/**
 * 
 * Stores the file details in terms of chunks from Mongo.
 *
 */
public class MessageDetails {
	private String fileName;
	private byte[] byteData;
	private int noOfChuncks;
	private int chunckId;

	public MessageDetails(String fileName, byte[] bytedata, int noofchunck, int chunckid) {
		this.fileName = fileName;
		this.byteData = bytedata;
		this.noOfChuncks = noofchunck;
		this.chunckId = chunckid;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public byte[] getByteData() {
		return byteData;
	}

	public void setByteData(byte[] byteData) {
		this.byteData = byteData;
	}

	public int getNoOfChuncks() {
		return noOfChuncks;
	}

	public void setNoOfChuncks(int noOfChuncks) {
		this.noOfChuncks = noOfChuncks;
	}

	public int getChunckId() {
		return chunckId;
	}

	public void setChunckId(int chunckId) {
		this.chunckId = chunckId;
	}

}
