import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PageRank {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	    
		int iter = Integer.parseInt(args[0]);
		int reduce_jobs= Integer.parseInt(args[1]);
		String inputPathUrl=args[2];
		String outputPathUrl=args[3];
		
		
		for(int i=0;i<iter;i++)
		{
		Configuration conf = new Configuration ();
	    conf.set("mapred.compress.map.output", "true");
		
		Job job = new Job (conf);
		String jobName = "PageRank"+i; 
	    job.setJobName (jobName);
	    
	 // we'll output pagerank and url pairs (since we have words as keys and counts as values)
	    job.setMapOutputKeyClass (Text.class);
	    job.setMapOutputValueClass (Text.class);
	    
	    
	    // output text-text pairs - should be identical to Mapper output
	    job.setOutputKeyClass (Text.class);
	    job.setOutputValueClass (Text.class);
	    
	    
	    
	 // tell Hadoop the mapper and the reducer to use
	    job.setMapperClass (PageRankMapper.class);
	    //job.setCombinerClass (PageRankReducer.class);
	    job.setReducerClass (PageRankReducer.class);
	
	 // we'll be reading the text file we created in first part, so we can use Hadoop's built-in TextInputFormat
	    job.setInputFormatClass (SequenceFileInputFormat.class);
	    
	 
	  
	    
	 // set the input and output paths
	    
	    StringBuilder inputPath = new StringBuilder();
	    StringBuilder outputPath = new StringBuilder();
	    if (i==0)
	    {
	    	inputPath.append(inputPathUrl+"/*");
	    }
	    else
	    {
	    	inputPath.append(outputPathUrl+"/pass"+i+"///*");
	    }
	    
	   
	    SequenceFileInputFormat.addInputPaths(job,inputPath.toString());
	    outputPath.append(outputPathUrl+"/pass"+(i+1));
	    if(i==(iter-1))
	    {
	    	// we can use Hadoop's built-in TextOutputFormat for writing out the output text file
		    job.setOutputFormatClass (TextOutputFormat.class);  
	    	TextOutputFormat.setOutputPath (job, new Path (outputPath.toString())); 
	    	TextOutputFormat.setCompressOutput(job,true);
	    }
	    else
	    {
	    	// we can use Hadoop's built-in TextOutputFormat for writing out the output text file
		    job.setOutputFormatClass (SequenceFileOutputFormat.class);  
	    	SequenceFileOutputFormat.setOutputPath (job, new Path (outputPath.toString())); 
	    	SequenceFileOutputFormat.setCompressOutput(job,true);
	    }
	    // set the input and output paths
	    job.setJarByClass(PageRank.class);
	    job.setNumReduceTasks (reduce_jobs);
	    job.waitForCompletion(true);
		
		}
	    	
	}

}
