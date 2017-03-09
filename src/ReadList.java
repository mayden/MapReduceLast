

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * This class generates the list of the stop words from a given file
 *
 */
public class ReadList {

	public static List<String> StopWords(String filename) {
		InputStream is = null;
		BufferedReader in = null;
		List<String> list = new ArrayList<String>();

		try {
			is = InitialRun.class.getClassLoader().getResourceAsStream(filename);
			in = new BufferedReader(new InputStreamReader(is));
			String str;
			while ((str = in.readLine()) != null) {
				list.add(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

}
