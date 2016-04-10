package gash.server.util;

public class Constants {
	/*
	 * Database constants
	 */
	public static final String PERSISTENCE_DATABASE = "Fluffy";
	public static final String DATABASE_COLLECTION = "Fluffy";
	public static final String COLLECTION_FIELD_BYTES = "bytes";
	public static final String COLLECTION_FIELD_NO_OF_CHUNKS = "noOfChuncks";
	public static final String COLLECTION_FIELD_CHUNK_ID = "chunckId";
	public static final String QUERY_DOCUMENT_FILENAME = "fileName";

	public static final String MONGO_HOST = "localhost";
	public static final int MONGO_PORT = 27017;

	/*
	 * Raft Constants
	 */
	public static final int MINIMUM_NUMBER_OF_NODES_REQUIRED = 3;

	public static final String MONITOR_HOST = "169.254.1.2";
	public static final int MONITRO_PORT = 5000;

}
