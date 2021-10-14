package BigDataProject1;


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


public class InMapperAvgComb {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Pair> {
	    private java.util.Map<String, Pair> avgcombining;
	    
	    public void setup(Context context){
			   avgcombining = new HashMap<>();
		   }
	     
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
		    		Integer.parseInt(ave);
		    		number= ave;
		    	}catch(NumberFormatException e){ ; }
		    }
		    if(avgcombining.containsKey(ip)){
			   Pair pair = (Pair) avgcombining.get(ipnum);
			   pair.addSum(Integer.valueOf(number));
			   pair.addCount(1);
			   avgcombining.put(ipnum, pair);
		   }
		   else avgcombining.put(ipnum, new Pair(Integer.valueOf(number), 1));
	    }
	    
	    public void cleanup(Context context) throws IOException, InterruptedException{
		   Iterator<java.util.Map.Entry<String, Pair>> temp = avgcombining.entrySet().iterator();
		   
		   while(temp.hasNext()){
			   java.util.Map.Entry<String, Pair> entry = temp.next();
			   String keyValue= entry.getKey();
			   Pair value = entry.getValue();
			   context.write(new Text(keyValue), value);
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
	    Configuration configurations = new Configuration();
	     
	    Job teamjob = new Job(configurations, "avgcombwordcount");
	    teamjob.setJarByClass(InMapperAvgComb .class);
	    
	    teamjob.setOutputKeyClass(Text.class);
	    teamjob.setOutputValueClass(DoubleWritable.class);
	    teamjob.setMapOutputValueClass(Pair.class);
	     
	    teamjob.setMapperClass(Map.class);
	    teamjob.setReducerClass(Reduce.class);
	     
	    teamjob.setInputFormatClass(TextInputFormat.class);
	    teamjob.setOutputFormatClass(TextOutputFormat.class);
	     
	    FileInputFormat.addInputPath(teamjob, new Path(args[0]));
	    FileOutputFormat.setOutputPath(teamjob, new Path(args[1]));
	     
	    teamjob.waitForCompletion(true);
	 }

}


