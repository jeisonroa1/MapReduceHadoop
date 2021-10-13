package Bigdataprojectt;



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

public class InMapComWordCount {
	
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	   private java.util.Map<String, Integer> combiningMap;
	   private Text word = new Text();
	   
	   public void setup(Context context){
		   combiningMap = new HashMap<>();
	   }
	   
	   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	       String line = value.toString();
		   StringTokenizer tokenizer = new StringTokenizer(line);
		   while (tokenizer.hasMoreTokens()) {
		       String token = tokenizer.nextToken();
			   if(combiningMap.containsKey(token)){
				   int sum = (int) combiningMap.get(token) + 1;
				   combiningMap.put(token, sum);
			   }
			   else combiningMap.put(token, 1);
		   }
	   }
	   
	   public void cleanup(Context context) throws IOException, InterruptedException{
		   Iterator<java.util.Map.Entry<String, Integer>> temp = combiningMap.entrySet().iterator();
		   
		   while(temp.hasNext()){
			   java.util.Map.Entry<String, Integer> entry = temp.next();
			   String keyVal = entry.getKey();
			   Integer val = entry.getValue();
			   context.write(new Text(keyVal), new IntWritable(val));
		   }
	   }
	}
	    
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	   public void reduce(Text key, Iterable<IntWritable> values, Context context)
	     throws IOException, InterruptedException {
	       int sum = 0;
		   for (IntWritable val : values) {
		       sum += val.get();
		   }
		   context.write(key, new IntWritable(sum));
	   }
	}
	    
	public static void main(String[] args) throws Exception {
	   Configuration conf = new Configuration();
	    
	   Job job = new Job(conf, "wordcount");
	   job.setJarByClass(InMapComWordCount.class);
	    
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


