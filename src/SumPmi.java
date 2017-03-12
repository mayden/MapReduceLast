import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SumPmi {
	private static final String MAIN_DEL = "#";
	private static final String SPACE_DEL = " ";
	public static class MapperClass extends MapReduceBase implements Mapper<Text, Text, Text, DoubleWritable> {

		@Override
		public void map(Text dataset, Text pmi, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			String decade = dataset.toString().split(SPACE_DEL)[2];
			double pmi_value = Double.parseDouble(pmi.toString());
			output.collect(new Text(decade), new DoubleWritable(pmi_value));

		}

	}

	public static class ReducerClass extends MapReduceBase
			implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text decade, Iterator<DoubleWritable> pmi, OutputCollector<Text, DoubleWritable> output,
				Reporter reporter) throws IOException {

			double sum = 0;
			while (pmi.hasNext()) {
				sum += pmi.next().get();
			}
			output.collect(decade, new DoubleWritable(sum));

		}
	}

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();
		JobConf jobConf = new JobConf(conf);
		jobConf.setJarByClass(CalcPmi.class);
		jobConf.setMapperClass(MapperClass.class);
		jobConf.setReducerClass(ReducerClass.class);
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setNumReduceTasks(1);
		jobConf.setOutputFormat(TextOutputFormat.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(DoubleWritable.class);
		JobClient.runJob(jobConf);
	}

}
