import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

 
public class InitialRun { 
   
    
public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    
	public static final Log LOG = LogFactory.getLog(MapClass.class);
    public static String isStopWords;
    public static String lang;
    
    private static List<String> stopWords = ReadList.StopWords("engWords.txt");
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      
      String[] tokenValues = value.toString().split("\t"); 
      String pair = tokenValues[0];
      
      int year = Integer.parseInt(tokenValues[1]);
      int decade = year - (year % 10);
      
      String firstWord = pair.split(" ")[0].trim();
      String secondWord = pair.split(" ")[1].trim(); 
      
      // Check the length of the pair (if not null)
      if(firstWord.length() == 0 || secondWord.length() == 0) return;
      if(!isValid(firstWord) || !isValid(secondWord)) return;    
      
      // Check if we need StopWords
      // 0 ==> We NOT including the Stop Words, and we MESANENIM! (filter)
      if(isStopWords.equals("0") &&  (stopWords.contains(firstWord.toLowerCase()) ||  stopWords.contains(secondWord.toLowerCase()))) return;
    	 
      Text outputText = new Text(firstWord + " " + secondWord + " " + decade);
      IntWritable countInt = new IntWritable(Integer.parseInt(tokenValues[2]));
      
      output.collect(outputText, countInt);
      
    } // end .. void map..
    
    private boolean isValid(String word) {

    	if( ('א' <= word.charAt(0) & word.charAt(0) <= 'ת') | ('A' <= word.charAt(0) & word.charAt(0) <= 'Z') | ('a' <= word.charAt(0) & word.charAt(0) <= 'z'))
    		return true;
    	
		return false;

	}


	public void configure(JobConf job) {
    	MapClass.isStopWords = job.get("isStopWords");
    	MapClass.lang = job.get("lang");
    }

    
  } // end class
 
  public static class ReduceClass extends MapReduceBase implements Reducer<Text,IntWritable,Text,Text> {


	public void reduce(Text dataset, Iterator<IntWritable> iterator, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		int totalNumber = 0;
		int countNumber = 0;
		while(iterator.hasNext())
		{
			countNumber++;
			totalNumber += iterator.next().get();
		}
		
		Text outputText = new Text(totalNumber + " " + countNumber);
		
		output.collect(dataset, outputText);
	}

  }
 

 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    JobConf job = new JobConf(conf);
    
    job.set("lang", args[2]);
    job.set("isStopWords", args[3]);

    job.setJarByClass(InitialRun.class);
    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setInputFormat(SequenceFileInputFormat.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    // submit job
    JobClient.runJob(job);
    
  }
 
}