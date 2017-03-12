import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

 
public class FinalCount { 
   
	private static final String MAIN_DEL = "#";
	private static final String SPACE_DEL = " ";
	private static final String ZERO_DEL = "0";
    
public static class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	public void map(Text dataset, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
	    
		String[] parsedData = value.toString().split(MAIN_DEL);
		String count = parsedData[1];
		
		
		String[] data = dataset.toString().split(SPACE_DEL);
		
		String firstWord = data[0];
		String secondWord = data[1];
		String decade = data[2];
		
		Text outputText = new Text(firstWord + SPACE_DEL + secondWord + SPACE_DEL + count);
		output.collect(new Text(decade), outputText);
		
    } 
   

    
  } // end class
 
  public static class ReduceClass extends MapReduceBase implements Reducer<Text,Text,Text,IntWritable> {

	@Override
	public void reduce(Text data, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		int totalCount = 0;
		
		while(values.hasNext()){
			String value = values.next().toString();
			String[] parsedData = value.split(SPACE_DEL);
			
			int countValue   = Integer.parseInt(parsedData[2]);
			
			totalCount += countValue;
		}
		
		output.collect(data, new IntWritable(totalCount));
		
	}	
}






 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    JobConf job = new JobConf(conf);

    job.setJarByClass(FinalCount.class);
    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
	 
    job.setInputFormat(KeyValueTextInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    // submit job
    JobClient.runJob(job);
    
  }
 
}