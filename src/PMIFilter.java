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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.*;

public class PMIFilter {
	public static class MapperClass extends MapReduceBase implements Mapper<Text, Text, Text, DoubleWritable> {

		private static double minPMI = 0;
		public static double relMinPMI = 0;
		private Path localCachePath;
		public static HashMap<String, String> sum_values = null;

		@Override
		public void map(Text dataset, Text data, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {

			String decade = dataset.toString().split(" ")[2];

			double pmi = Double.parseDouble(data.toString());
			double sum = Double.parseDouble(MapperClass.sum_values.get(decade));

			if ((pmi >= minPMI) && (pmi / sum >= relMinPMI)) {
				output.collect(new Text(dataset.toString() + " " + data.toString()), new DoubleWritable(pmi));
			}

		}

		@Override
		public void configure(JobConf conf) {			
			try {
				localCachePath = DistributedCache.getLocalCacheFiles(conf)[0];
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			try {
				sum_values = new CacheHashMapGenerator(new FileInputStream(localCachePath.toString())).getHashMap();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			MapperClass.minPMI = Double.parseDouble(conf.get("minPMI"));
			MapperClass.relMinPMI = Double.parseDouble(conf.get("relMinPMI"));
		}
	}

	public static class ReducerClass extends MapReduceBase
			implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text bigram, Iterator<DoubleWritable> pmi, OutputCollector<Text, DoubleWritable> output,
				Reporter reporter) throws IOException {

			while (pmi.hasNext()) {

				String[] bigramAnddecade = bigram.toString().split(" ");

				output.collect(new Text(bigramAnddecade[0] + " " + bigramAnddecade[1] + " " + bigramAnddecade[2]),
						pmi.next());
			}

		}
	}


	public static class BigramComparator extends WritableComparator {

		protected BigramComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text data1 = (Text) w1;
			Text data2 = (Text) w2;

			String[] bigramAndDecade1 = data1.toString().split(" ");
			String[] bigramAndDecade2 = data2.toString().split(" ");

			String decade1 = bigramAndDecade1[2];
			String decade2 = bigramAndDecade1[2];

			int compareDecade = decade1.compareTo(decade2);

			if (compareDecade != 0) {
				return compareDecade;
			}

			double pmi1 = Double.parseDouble(bigramAndDecade1[3]);
			double pmi2 = Double.parseDouble(bigramAndDecade2[3]);
			int compare_pmi = Double.compare(pmi1, pmi2);

			if (compare_pmi != 0) {
				return compare_pmi;
			}
			String bigram1 = bigramAndDecade1[0] + " " + bigramAndDecade1[1];
			String bigram2 = bigramAndDecade2[0] + " " + bigramAndDecade2[1];

			return bigram1.compareTo(bigram2);

		}

	}

	public static void main(String[] args) throws IOException, URISyntaxException {

		Configuration conf = new Configuration();

		JobConf jobConf = new JobConf(conf);

		jobConf.set("minPMI", args[2]);
		jobConf.set("relMinPMI", args[3]);

		jobConf.setOutputKeyComparatorClass(BigramComparator.class);
		//jobConf.setNumReduceTasks(1);

		jobConf.setJarByClass(CalcPmi.class);
		jobConf.setMapperClass(MapperClass.class);
		jobConf.setReducerClass(ReducerClass.class);
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(DoubleWritable.class);

		DistributedCache.addCacheFile(new URI(args[4]), jobConf);

		JobClient.runJob(jobConf);
	}

}
