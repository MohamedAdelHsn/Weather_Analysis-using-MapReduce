package com.mr.weatherAnalysis;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends Reducer<Text, IntWritable , Text, IntWritable> {

	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	    int MAX_Temp = Integer.MIN_VALUE;
		
	    for(IntWritable value : values) 
	    {	     
	    	MAX_Temp = Math.max(MAX_Temp, value.get());	    	    	
	    }
		
	    context.write(key, new IntWritable(MAX_Temp));
		
	}
}
