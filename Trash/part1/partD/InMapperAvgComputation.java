package org.myorg;

import java.io.DataInput;
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

public class InMapperAvgComputation {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Pair> {
	    private java.util.Map<String, Pair> combiningMap;
	    
	    public void setup(Context context){
			   combiningMap = new HashMap<>();
		   }
	     
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
		    if(combiningMap.containsKey(ip)){
			   Pair p = (Pair) combiningMap.get(ip);
			   p.addSum(Integer.valueOf(num));
			   p.addCount(1);
			   combiningMap.put(ip, p);
		   }
		   else combiningMap.put(ip, new Pair(Integer.valueOf(num), 1));
	    }
	    
	    public void cleanup(Context context) throws IOException, InterruptedException{
		   Iterator<java.util.Map.Entry<String, Pair>> temp = combiningMap.entrySet().iterator();
		   
		   while(temp.hasNext()){
			   java.util.Map.Entry<String, Pair> entry = temp.next();
			   String keyVal = entry.getKey();
			   Pair val = entry.getValue();
			   context.write(new Text(keyVal), val);
		   }
		}
	 }
	     
	 public static class Reduce extends Reducer<Text, Pair, Text, DoubleWritable> {
	 
	    public void reduce(Text key, Iterable<Pair> values, Context context)
	      throws IOException, InterruptedException {
	        int sum = 0, cnt = 0;
		    for (Pair val : values) {
		    	sum += val.getSum();
		        cnt += val.getCount();
		    }
		    double avg = ((double) sum)/cnt;
		    context.write(key, new DoubleWritable(avg));
	    }
	 }
	 
	 public static class Pair implements Writable{
		 public int sum;
		 public int count;
		 public Pair(){}
		 public Pair(int sum, int count){
			 this.sum = sum;
			 this.count = count;
		 }
		 public void addSum(int sum){ this.sum += sum; }
		 public void addCount(int count){ this.count += count; }
		 public int getSum(){ return sum; }
		 public int getCount(){ return count; }
		 
		 public void write(java.io.DataOutput out) throws IOException{
			 out.writeInt(sum);
			 out.writeInt(count);
		 }
		 public void readFields(DataInput in) throws IOException{
			 sum = in.readInt();
			 count = in.readInt();
		 }
		 public String toString(){
			 return "Pair("+Integer.toString(sum)+", "+Integer.toString(count)+")"; 
		 }
	 }
	     
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	     
	    Job job = new Job(conf, "wordcount");
	    job.setJarByClass(InMapperAvgComputation.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    job.setMapOutputValueClass(Pair.class);
	     
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	     
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	     
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	     
	    job.waitForCompletion(true);
	 }

}