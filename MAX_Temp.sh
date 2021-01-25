#!/bin/bash
#simple script to run mapreduce job on hadoop
# Mohamed Adel Hsn  :> coded at 25/01/2021 , 11:00PM
# first put or copy your dataset from local into hdfs
hadoop fs -put /home/hdpuser/Desktop/dataset/sample.txt hdfs://localhost:9000/Test
# run your own mapreduce job by passing jar file into hadoop with the following schema >> hadoop jar </path_of_the_jar_file>  <jarclass_name> <input_data> <output_data>
hadoop jar '/home/hdpuser/Desktop/jars/Max_Temp.jar' Max_Temp /Test/sample.txt /output
# print the output after processing 
hadoop fs -cat /output/part-r-00000
