import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

 
public class DataCount { 
   
	private static final String MAIN_DEL = "#";
	private static final String LIST_DEL = ",";
	private static final String ZERO_DEL = "0";
    
public static class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	public void map(Text dataset, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
		String[] parsedData = value.toString().split(MAIN_DEL);
		
		String count_1 = parsedData[0];
		String count_2 = parsedData[1];
		
		
		if (!parsedData[2].equals(ZERO_DEL)){				
			String[] startList = parsedData[2].split(LIST_DEL);
			
			for(String word1 : startList){
				String[] values = word1.split("\\(");
				
				Text outputText = new Text(values[1] + MAIN_DEL + count_1 + MAIN_DEL + ZERO_DEL);
				
				output.collect(new Text(values[0]), outputText);
			}
		}
		
		if(!parsedData[3].equals(ZERO_DEL)){				
			String[] endList = parsedData[3].split(LIST_DEL);
			
			for(String word2 : endList){
				String values[] = word2.split("\\(");
				
				Text outputText2 =  new Text(values[1] + MAIN_DEL + ZERO_DEL + MAIN_DEL + count_2);
				output.collect(new Text(values[0]), outputText2);
			}
		}
    } 
   

    
  } // end class
 
  public static class ReduceClass extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

	@Override
	public void reduce(Text data, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		int totalCount	= 0;
		int c1_count		= 0;
		int c2_count		= 0;
		
		while(values.hasNext()){
			String value = values.next().toString();
			String[] parsedData = value.split(MAIN_DEL);
			
			int sum = Integer.parseInt(parsedData[0]);
			int count1	  = Integer.parseInt(parsedData[1]);
			int count2   = Integer.parseInt(parsedData[2]);
			
			c1_count += count1;
			c2_count += count2;
			totalCount += sum;
		}
		
		output.collect(data, new Text(Integer.toString(totalCount) + MAIN_DEL + Integer.toString(c1_count) + MAIN_DEL +Integer.toString(c2_count)));
	}	
}






 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    JobConf job = new JobConf(conf);

    job.setJarByClass(FirstCount.class);
    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setInputFormat(KeyValueTextInputFormat.class);
    
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    // submit job
    JobClient.runJob(job);
    
  }
 
}