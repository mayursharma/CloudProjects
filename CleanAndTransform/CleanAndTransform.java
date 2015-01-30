import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class CleanAndTransform {

	public static void main(String[] args) throws Exception {
		 
		int reduce_jobs=Integer.parseInt(args[0]);
		String output_path = args[1];
		
		// Get the default configuration object
	    Configuration conf = new Configuration ();
	    
		Job job = new Job (conf);
	    job.setJobName ("CleanAndTransform");
	    
	 // output text-text pairs (since we have BaseUrls as keys and LinkingUrls as values)
	    job.setMapOutputKeyClass (Text.class);
	    job.setMapOutputValueClass (Text.class);
	    
	 // output text-text pairs - should be identical to Mapper output
	    job.setOutputKeyClass (Text.class);
	    job.setOutputValueClass (Text.class);
	 
	 // tell Hadoop the mapper to use
	    job.setMapperClass (CleanAndTransformMapper.class);
	    //job.setCombinerClass (CleanAndTransformReducer.class);
	    job.setReducerClass (CleanAndTransformReducer.class);
	    
	 // we'll be reading a sequence file, so we can use Hadoop's built-in SequenceFileInputFormat
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    //job.setInputFormatClass (SequenceFileInputFormat.class);
	    
	 // we can use Hadoop's built-in TextOutputFormat for writing out the output text file
	    job.setOutputFormatClass (SequenceFileOutputFormat.class);   
	    
	 // set the input and output paths
	    
	    StringBuilder inputPath = new StringBuilder();
	    
	    
	      
	     for(int i=0;i<=300;i++)
	    {
	    	if (i<10)
	    		inputPath.append("s3://aws-publicdatasets/common-crawl/parse-output/segment/1346876860819/metadata-0000"+i+",");
	    	else if (i<100)
	    		inputPath.append("s3://aws-publicdatasets/common-crawl/parse-output/segment/1346876860819/metadata-000"+i+",");	
	    	else
	    		inputPath.append("s3://aws-publicdatasets/common-crawl/parse-output/segment/1346876860819/metadata-00"+i+",");	
	    }	
	    	
	    inputPath.deleteCharAt(inputPath.length()-1);
	    
	    
	    SequenceFileInputFormat.setInputPaths (job, inputPath.toString());
	    SequenceFileOutputFormat.setOutputPath (job, new Path (output_path));
	      
	 // this tells Hadoop to ship around the jar file containing "CleanAndTransform.class" to all of the different
	 // nodes so that they can run the job
	    job.setJarByClass(CleanAndTransform.class);
	
	    job.setNumReduceTasks (reduce_jobs);
	    
	 // submit the job and wait for it to complete!
	    job.waitForCompletion(true);	 
		 

	}

}
