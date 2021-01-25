package com.mr.weatherAnalysis;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper extends Mapper<LongWritable , Text , Text , IntWritable> {

	private static final int MISSING = 9999;
	
  /* record of data & data description	
   
   0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+00221+99999999999
   0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9-00111+99999999999
   0043012650999991949032412004+62300+010750FM-12+048599999V0202701N00461220001CN0500001N9+01111+99999999999
   0043012650999991949032418004+62300+010750FM-12+048599999V0202701N00461220001CN0500001N9+00781+99999999999

  */
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		 String line = value.toString();
		 String year = line.substring(15, 19);
		 int temp;

		 if (line.charAt(87) == '+') { 
			
           temp = Integer.parseInt(line.substring(88, 92));
         } else {
           temp = Integer.parseInt(line.substring(87, 92));
         }
		 
		 String quality = line.substring(92, 93);
			if (temp != MISSING && quality.matches("[01459]")) {
				context.write(new Text(year), new IntWritable(temp));
			}
		
		
	}
	
	
}
