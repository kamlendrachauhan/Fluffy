package gash.router.cache;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;


public class RiakHandler {
	
	/*This Class will be used to store data which has not been chunked as its FileSize is less than 1 MB*/
	
	protected static Logger logger = LoggerFactory.getLogger("RiakHandler");
	
    private static RiakCluster setUpCluster() throws UnknownHostException {
        RiakNode node = new RiakNode.Builder().withRemoteAddress("127.0.0.1").build();
        RiakCluster cluster = new RiakCluster.Builder(node).build();
        cluster.start();
        return cluster;
    }
	
	private static class RiakFile{
		public String fileName;
		public byte[] byteData;
	}
	
	//Call this method for storing file
	public static void storeFile(String fileName,byte[] byteData){
		try{
			RiakCluster cluster = setUpCluster();
            RiakClient client = new RiakClient(cluster);
            RiakFile newFile = createFile(fileName, byteData);  
            Namespace fileBucket = new Namespace("files");
            Location fileLocation = new Location(fileBucket, fileName);
            StoreValue storeFile = new StoreValue.Builder(newFile).withLocation(fileLocation).build();
            client.execute(storeFile);   
            cluster.shutdown();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		
	}
	
	  
	private static RiakFile createFile(String fileName,byte[] byteData)
	{
		RiakFile new_file = new RiakFile();
		new_file.fileName = fileName;
		new_file.byteData = byteData;
		return new_file;
	}
	
	//Call this method to get a file in byte[]
	public static byte[] getFile(String fileName){
		try {
			RiakCluster cluster = setUpCluster();
	        RiakClient client = new RiakClient(cluster);   
	        Namespace fileBucket = new Namespace("files");
            Location fileLocation = new Location(fileBucket, fileName);
	        FetchValue fetchFile = new FetchValue.Builder(fileLocation).build();
	        //RiakFile fetchedFile = client.execute(fetchFile).getValue(RiakFile.class);
	        RiakFile fetchedFile = new RiakFile();
	        logger.info("Currently fetched Riak file is null");
	        System.out.println(fetchedFile.fileName);
	        cluster.shutdown();
	        return(fetchedFile.byteData);
				
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static String readAll(Reader rd) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
	  }

  private static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
	    InputStream is = new URL(url).openStream();
	    try {
	      BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
	      String jsonText = readAll(rd);
	      JSONObject json = new JSONObject(jsonText);
	      return json;
	    } finally {
	      is.close();
	    }
	  }

	public ArrayList<String> getAllFileNames()
	{
		try {
			JSONObject json = readJsonFromUrl("http://127.0.0.1:8098/buckets/files/keys?keys=true");
		    JSONArray fileArray = json.getJSONArray("keys");
		    
		    ArrayList<String> filenames = new ArrayList<String>();
		    if (fileArray != null) { 
		    	   int len = fileArray.length();
		    	   for (int i=0;i<len;i++){ 
		    		   filenames.add(fileArray.get(i).toString());
		    	   } 
		    	} 
		    return(filenames);
		    
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
		
		
	}

	
	public static void main(String[] args)  {
        
		
		try{
           
	       RiakHandler rh = new RiakHandler();
	       //rh.storeFile("File2", new String("testFile").getBytes());
	       //System.out.println(rh.getFile("File4"));
	      // System.out.println(rh.getAllFileNames());
	      
	       
	      
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	
	
}