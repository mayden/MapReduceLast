import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class CalcPmi {
	private static final String MAIN_DEL = "#";
	private static final String SPACE_DEL = " ";
	public static class MapperClass extends MapReduceBase implements Mapper<Text, Text, Text, DoubleWritable> {

		public static HashMap<String, String> decade_to_N = null;

		private Path localCachePath;

		@Override
		public void map(Text key, Text counters, OutputCollector<Text, DoubleWritable> output,
				Reporter reporter) throws IOException {

			String decade = key.toString().split(SPACE_DEL)[2];
			int N = Integer.parseInt(MapperClass.decade_to_N.get(decade));

			String[] parsedData = counters.toString().split(MAIN_DEL);

			int bigramCount = Integer.parseInt(parsedData[0]);
			int c_w_1 = Integer.parseInt(parsedData[1]);
			int c_w_2 = Integer.parseInt(parsedData[2]);

			double pmi = Math.log(bigramCount) + Math.log(N) - Math.log(c_w_1) - Math.log(c_w_2);

			output.collect(key, new DoubleWritable(pmi));

		}

	
		@Override
		public void configure(JobConf job) {
			try {
				localCachePath = DistributedCache.getLocalCacheFiles(job)[0];
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			try {
				decade_to_N =new CacheHashMapGenerator(new FileInputStream(localCachePath.toString())).getHashMap();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static class ReducerClass extends MapReduceBase
			implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text bigram, Iterator<DoubleWritable> pmi, OutputCollector<Text, DoubleWritable> output,
				Reporter reporter) throws IOException {

			while (pmi.hasNext()) {
				output.collect(bigram, pmi.next());
			}

		}
	}

	public static void main(String[] args) throws IOException, URISyntaxException {

		System.out.println("args[2]:" + args[2]);
		Configuration conf = new Configuration();

		JobConf jobConf = new JobConf(conf);

		jobConf.setJarByClass(CalcPmi.class);
		jobConf.setMapperClass(MapperClass.class);
		jobConf.setReducerClass(ReducerClass.class);
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(DoubleWritable.class);

		DistributedCache.addCacheFile(new URI(args[2]), jobConf);

		JobClient.runJob(jobConf);
	}

}
