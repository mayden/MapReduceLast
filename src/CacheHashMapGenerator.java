import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;


public class CacheHashMapGenerator{

	private HashMap<String, String> hashMap;
	
	public CacheHashMapGenerator(InputStream input) throws IOException{
		
		this.hashMap = new HashMap<String, String>();
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));
		String line = null;
		while((line = bufferedReader.readLine()) != null){
			String[] items = line.split("\t");
			this.hashMap.put(items[0], items[1]);
		}		
	}
	public HashMap<String, String> getHashMap (){return hashMap;}

}
