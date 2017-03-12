import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

 
public class FirstCount { 
   
	private static final String MAIN_DEL = "#";
	private static final String LIST_DEL = ",";
	private static final String ZERO_DEL = "0";
    
public static class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	
	public void map(Text dataset, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
		String[] parsedData = value.toString().split(" ");
		String sumSoFar = parsedData[0];
		
		String[] counts = dataset.toString().split(" ");
		
		String firstWord	= counts[0];
		String secondWord	= counts[1];
		String decade		= counts[2];
		
		// Example: years 1960      32#0#London years 1960(0#0
		
		output.collect(new Text(firstWord + " " + decade), new Text(sumSoFar + MAIN_DEL +  ZERO_DEL + MAIN_DEL + dataset.toString() + "(" + ZERO_DEL + MAIN_DEL + ZERO_DEL ));

		// Example: years 1960      0#32#[0]#London years 1960(32
		// Meaning: 0 - start, 32 - summing the counts of the word so far, [0] - length of the book list
		//			London years 1960 - the full book name
		
		Text outputText = new Text(ZERO_DEL + MAIN_DEL + sumSoFar  + MAIN_DEL + ZERO_DEL + MAIN_DEL + dataset.toString() + "(" + sumSoFar);
		
		output.collect(new Text(secondWord + " " + decade), outputText);
      
    } 
   

    
  } // end class
 
  public static class ReduceClass extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

	@Override
	public void reduce(Text data, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		boolean startFlag = false;
		boolean endFlag = false;
		int startCount = 0;
		int endCount   = 0;
		StringBuilder startData = new StringBuilder();
		StringBuilder endData = new StringBuilder();
		
		
		while(values.hasNext()){
			String value = values.next().toString();
			String[] parsedData = value.split(MAIN_DEL);
			
			int countStart = Integer.parseInt(parsedData[0]);
			int countEnd	 = 	Integer.parseInt(parsedData[1]);
			String startDataList  = 	parsedData[2];
			String endDataList	 =	 parsedData[3];
			
			startCount += countStart;
			endCount   += countEnd;
			
			
			if (!startDataList.equals(ZERO_DEL)){
				if (startFlag) 
					startData.append(LIST_DEL);
				startData.append(startDataList);
				startFlag = true;
			}
			
			if (!endDataList.equals(ZERO_DEL)){
				if (endFlag) 
					endData.append(LIST_DEL);
				endData.append(endDataList);
				endFlag = true;
			}
		}
		
		if (startData.length() < 1) 
			startData.append(ZERO_DEL);
		
		if (endData.length() < 1) 
			endData.append(ZERO_DEL);
		
		Text outputText = new Text(startCount + MAIN_DEL + endCount + MAIN_DEL + startData.toString() + MAIN_DEL + endData.toString());
		output.collect(data,outputText);
		
	}




  }
 

 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    conf.set("key.value.separator.in.input.line","\t");
    conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
    
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