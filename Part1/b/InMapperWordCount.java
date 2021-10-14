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


public class MapComWordCount {
	
	
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		   private java.util.Map<String, Integer> MapCom;
		   private Text word = new Text();
		   
		   public void setup(Context context){
			   MapCom = new HashMap<>();
		   }
		   
		   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		       String line = value.toString();
			   StringTokenizer tokenizer = new StringTokenizer(line);
			   while (tokenizer.hasMoreTokens()) {
			       String token = tokenizer.nextToken();
				   if(MapCom.containsKey(token)){
					   int sum = (int) MapCom.get(token) + 1;
					   MapCom.put(token, sum);
				   }
				   else MapCom.put(token, 1);
			   }
		   }
		   
		   public void cleanup(Context context) throws IOException, InterruptedException{
			   Iterator<java.util.Map.Entry<String, Integer>> temp = MapCom.entrySet().iterator();
			   
			   while(temp.hasNext()){
				   java.util.Map.Entry<String, Integer> entry = temp.next();
				   String keyValue = entry.getKey();
				   Integer value = entry.getValue();
				   context.write(new Text(keyValue), new IntWritable(value));
			   }
		   }
		}
		    
		public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		   public void reduce(Text key, Iterable<IntWritable> values, Context context)
		     throws IOException, InterruptedException {
		       int sum = 0;
			   for (IntWritable value : values) {
			       sum += value.get();
			   }
			   context.write(key, new IntWritable(sum));
		   }
		}
		    
		public static void main(String[] args) throws Exception {
		   Configuration configurations = new Configuration();
		    
		   Job teamjob = new Job(configurations, "mapcomwordcount");
		   teamjob.setJarByClass(MapComWordCount.class);
		    
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


