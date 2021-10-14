package BigDataProject;

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

public class AverageComb {


public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		    private Text words= new Text();
		     
		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		        String line = value.toString();
			    StringTokenizer tokenizer = new StringTokenizer(line);
			    boolean result = true;
			    String ipnum = "", number = "", ave;
			    
			    while (tokenizer.hasMoreTokens()) {
			        ave = tokenizer.nextToken();
			    	if(result){
			        	ipnum = ave;
			        	result = false;
			        }
			        try{
			    		Integer.parseInt(aux);
			    		number = ave;
			    	}catch(NumberFormatException e){ ; }
			    }
			    System.out.println(ipnum+", "+number);
			    words.set(ip);
		        context.write(words, new IntWritable(Integer.valueOf(number)));
		    }
		 }
		     
		 public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		 
		    public void reduce(Text key, Iterable<IntWritable> values, Context context)
		      throws IOException, InterruptedException {
		        int sum = 0, cnt = 0;
			    for (IntWritable value : values) {
			        sum += value.get();
			        cnt++;
			    }
			    double avg = ((double) sum)/cnt;
			    context.write(key, new DoubleWritable(average));
		    }
		 }
		     
		 public static void main(String[] args) throws Exception {
		    Configuration configurations = new Configuration();
		     
		    Job teamjob = new Job(configurations, "avgcomwordcount");
		    teamjob.setJarByClass(AverageComb.class);
		     
		    teamjob.setOutputKeyClass(Text.class);
		    teamjob.setOutputValueClass(IntWritable.class);
		     
		    teamjob.setMapperClass(Map.class);
		    teamjob.setReducerClass(Reduce.class);
		     
		    teamjob.setInputFormatClass(TextInputFormat.class);
		    teamjob.setOutputFormatClass(TextOutputFormat.class);
		     
		    FileInputFormat.addInputPath(teamjob, new Path(args[0]));
		    FileOutputFormat.setOutputPath(teamjob, new Path(args[1]));
		     
		    teamjob.waitForCompletion(true);
		 }

	}


