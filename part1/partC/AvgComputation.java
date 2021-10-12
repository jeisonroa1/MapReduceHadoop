package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AvgComputation {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private Text word = new Text();
	     
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
		    StringTokenizer tokenizer = new StringTokenizer(line);
		    boolean firstTime = true;
		    String ip = "", num = "", aux;
		    
		    while (tokenizer.hasMoreTokens()) {
		        aux = tokenizer.nextToken();
		    	if(firstTime){
		        	ip = aux;
		        	firstTime = false;
		        }
		        try{
		    		Integer.parseInt(aux);
		    		num = aux;
		    	}catch(NumberFormatException e){ ; }
		    }
		    System.out.println(ip+", "+num);
		    word.set(ip);
	        context.write(word, new IntWritable(Integer.valueOf(num)));
	    }
	 }
	     
	 public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	 
	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	      throws IOException, InterruptedException {
	        int sum = 0, cnt = 0;
		    for (IntWritable val : values) {
		        sum += val.get();
		        cnt++;
		    }
		    double avg = ((double) sum)/cnt;
		    context.write(key, new DoubleWritable(avg));
	    }
	 }
	     
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	     
	    Job job = new Job(conf, "wordcount");
	    job.setJarByClass(AvgComputation.class);
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	     
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	     
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	     
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	     
	    job.waitForCompletion(true);
	 }

}
