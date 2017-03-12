import com.amazonaws.auth.AWSCredentials;

import com.amazonaws.auth.BasicAWSCredentials;

import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.*;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;



public class Main {

	private static final String jarAddress = "s3n://dsps-jars-ass2/assignment2.jar";
	private static final String engFiles = "https://s3-us-west-2.amazonaws.com/dsps-jars-ass2/eng-us-all-100k-2gram";
	private static final String hebFiles = "https://s3-us-west-2.amazonaws.com/dsps-jars-ass2/heb-2gram-10k";
	private static final String outputAddress = "s3n://dsps-jars-ass2/output/";
    private static final String awsRegion = "us-east-1";
    
	private static String langAddress = "";
	
	public static void main(String[] args) {

		if(!isArgsValid(args))
		{
			System.out.println("Usage: java -jar extract.collations.using.mr.jar ExtractCollations minPmi relMinPmi heb/eng ToStopWords?(1/0)");
			return;
		}
		
		String minPmi = args[0];
		String relPMI = args[1];
		String lang = args[2];
		String toStopWords = args[3];
		
		// check which input we need (heb or eng)
		if(lang.equals("heb"))
			langAddress = hebFiles;
		else
			langAddress = engFiles;
		
    	AWSCredentials credentials =  new BasicAWSCredentials(AWSKeys.getAccessKey(),AWSKeys.getSecretKey());

    	
        AmazonElasticMapReduce emrClient = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSCredentialsImpl())
                .withRegion(awsRegion)
                .build();
        

        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
        
		// Example: Output: s3://bucket/heboutput1
		HadoopJarStepConfig initialRun = new HadoopJarStepConfig()
			    .withJar(jarAddress) // This should be a full map reduce application.
			    .withMainClass("InitialRun")
			    .withArgs(langAddress, outputAddress + lang + "output", lang, toStopWords);
			 
			StepConfig initialRunStep = new StepConfig()
			    .withName("InitialRun")
			    .withHadoopJarStep(initialRun)
			    .withActionOnFailure("TERMINATE_JOB_FLOW");
			

		HadoopJarStepConfig firstCount = new HadoopJarStepConfig()
				    .withJar(jarAddress) // This should be a full map reduce application.
				    .withMainClass("FirstCount")
				    .withArgs(outputAddress + lang + "output", outputAddress + lang + "output1");
				 
		StepConfig firstCountStep = new StepConfig()
				    .withName("FirstCount")
				    .withHadoopJarStep(firstCount)
				    .withActionOnFailure("TERMINATE_JOB_FLOW");
			
		// Example: Output: s3://bucket/heboutput1
		HadoopJarStepConfig dataCount = new HadoopJarStepConfig()
				    .withJar(jarAddress) // This should be a full map reduce application.
				    .withMainClass("DataCount")
				    .withArgs(outputAddress + lang + "output1", outputAddress + lang + "output2");
				 
		StepConfig dataCountStep = new StepConfig()
				    .withName("DataCount")
				    .withHadoopJarStep(dataCount)
				    .withActionOnFailure("TERMINATE_JOB_FLOW");
		

		HadoopJarStepConfig finalCount = new HadoopJarStepConfig()
				    .withJar(jarAddress) // This should be a full map reduce application.
				    .withMainClass("FinalCount")
				    .withArgs(outputAddress + lang + "output2", outputAddress + lang + "output3");
				 
		StepConfig finalCountStep = new StepConfig()
				    .withName("FinalCount")
				    .withHadoopJarStep(finalCount)
				    .withActionOnFailure("TERMINATE_JOB_FLOW");
		
		// Example: Output: s3://bucket/heboutput1
		HadoopJarStepConfig calcPMI = new HadoopJarStepConfig()
				    .withJar(jarAddress) // This should be a full map reduce application.
				    .withMainClass("CalcPmi")
				    .withArgs(outputAddress + lang + "output2", outputAddress + lang + "output4",  outputAddress + lang + "output3/part-00000");
				 
		StepConfig calcPMIStep = new StepConfig()
				    .withName("CalcPmi")
				    .withHadoopJarStep(calcPMI)
				    .withActionOnFailure("TERMINATE_JOB_FLOW");
		
		HadoopJarStepConfig sumPMI = new HadoopJarStepConfig()
			    .withJar(jarAddress) // This should be a full map reduce application.
			    .withMainClass("SumPmi")
			    .withArgs(outputAddress + lang + "output4", outputAddress + lang + "output5");
			 
		StepConfig sumPMIStep = new StepConfig()
			    .withName("SumPmi")
			    .withHadoopJarStep(sumPMI)
			    .withActionOnFailure("TERMINATE_JOB_FLOW");
		
		HadoopJarStepConfig PMIFilter = new HadoopJarStepConfig()
			    .withJar(jarAddress) // This should be a full map reduce application.
			    .withMainClass("PMIFilter")
			    .withArgs(outputAddress + lang + "output4", outputAddress + lang + "output6", minPmi, relPMI, outputAddress + lang + "output5/part-00000");
			 
		StepConfig PMIFilterStep = new StepConfig()
			    .withName("PMIFilter")
			    .withHadoopJarStep(PMIFilter)
			    .withActionOnFailure("TERMINATE_JOB_FLOW");

			 
			JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
			    .withInstanceCount(7)
			    .withMasterInstanceType(InstanceType.M1Medium.toString())
				.withSlaveInstanceType(InstanceType.M1Medium.toString())
			    .withHadoopVersion("2.6.2")
			    .withEc2KeyName("alex")
			    .withKeepJobFlowAliveWhenNoSteps(false);
			 
			RunJobFlowRequest runFlowRequest =  new RunJobFlowRequest()
			    .withName("CollocationExtractor")
			    .withInstances(instances)
			    .withSteps(initialRunStep,firstCountStep,dataCountStep,finalCountStep,calcPMIStep,sumPMIStep,PMIFilterStep)
			    .withJobFlowRole("EMR_EC2_DefaultRole")
				.withServiceRole("EMR_DefaultRole")
			    .withLogUri(outputAddress  + "logs/")
			    .withBootstrapActions();
			 

			RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
			String jobFlowId = runJobFlowResult.getJobFlowId();
			System.out.println("Ran job flow with id: " + jobFlowId);
			
	}

	private static boolean isArgsValid(String[] args) {
		if(args.length == 4)
		{
			// check language
			if(args[2].equals("eng") || args[2].equals("heb"))
			{
				// check to stop words or not
				if(args[3].equals("1") || args[3].equals("0"))
					return true;
				
				System.out.println("Please include the stop-words option: 1 or 0");
			}
			System.out.println("Please type correct language: heb or eng");
		}
		return false;
	}

}
