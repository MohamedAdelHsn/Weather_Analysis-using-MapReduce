package com.mr.weatherAnalysis;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Max_Temp {
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
	     if	(args.length!=	2){
			
	            System.err.println("Usage:	MaxTemperature	<input	path>	<output	path>");
		    System.exit(-1);				
		
		}	
		
	
           Job job = new Job();    
           job.setJobName("WordCount"); 
           job.setJarByClass(Max_Temp.class);
           job.setOutputKeyClass(Text.class);    
           job.setOutputValueClass(IntWritable.class);            
           job.setMapperClass(MaxTempMapper.class);    
           job.setCombinerClass(MaxTempReducer.class);    
           job.setReducerClass(MaxTempReducer.class);         
           job.setInputFormatClass(TextInputFormat.class);    
           job.setOutputFormatClass(TextOutputFormat.class);           
           FileInputFormat.setInputPaths(job,new Path(args[0]));    
           FileOutputFormat.setOutputPath(job,new Path(args[1]));              
           System.exit(job.waitForCompletion(true) ? 0 : 1);
	 	
		
	}

}
