package BigDataProject;

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

public class RelFreq {
	
	public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
	     
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
		    String[] words = line.split("\\s+");
		    for(int i=0; i<words.length-1; ++i){
		    	String u = words[i];
		    	String[] window = Arrays.copyOfRange(words, i+1, words.length);
		    	for(String v : window){
		    		if(v.equals(u)) break;
		    		context.write(new Pair(new Text(u), new Text(v)), one);
			    	context.write(new Pair(new Text(u), new Text("*")), one);
		    	}
		    }
	    }
	 }
	     
	public static class Reduce extends Reducer<Pair, IntWritable, Pair, Text> {
		int sum;
	    
		public void setup(Context context){
	    	sum = 0;
	    }
		
		public void reduce(Pair key, Iterable<IntWritable> values, Context context)
	      throws IOException, InterruptedException {
	        int cnt = 0;
		    for (IntWritable val : values) {
		        cnt += val.get();
		    }
		    if(key.getS2().equals(new Text("*"))) sum = cnt;
		    else{
		    	String rfreq = cnt+"/"+sum;
			    context.write(key, new Text(rfreq));
		    }
	    }
	 }
	 
	 public static class Pair implements WritableComparable<Pair>{
		 public Text s1;
		 public Text s2;
		 public Pair(){
			 this.s1 = new Text();
			 this.s2 = new Text();
		 }
		 public Pair(Text s1, Text s2){
			 this.s1 = s1;
			 this.s2 = s2;
		 }
		 public Text getS1(){ return s1; }
		 public Text getS2(){ return s2; }
		 
		 public void write(java.io.DataOutput out) throws IOException{
			 s1.write(out);
			 s2.write(out);
		 }
		 public void readFields(DataInput in) throws IOException{
			 s1.readFields(in);
			 s2.readFields(in);
		 }
		 public String toString(){
			 return "("+s1+", "+s2+")"; 
		 }
		@Override
		public int compareTo(Pair p) {
			int comp = this.s1.compareTo(p.s1);
			if(comp != 0) return comp;
			return this.s2.compareTo(p.s2);
		}
		@Override
		public boolean equals(Object o){
			if(this == o) return true;
			if(o == null || getClass() != o.getClass()) return false;
			Pair p = (Pair) o;
			if(s1 != null ? !s1.equals(p.s1) : p.s1 != null) return false;
			return s2 != null? s2.equals(p.s2) : p.s2 == null;
		}
		@Override
		public int hashCode(){
			int result = s1 != null ? s1.hashCode() : 0;
			result = 31* result + (s2 !=null ? s2.hashCode() : 0);
			return result;
		}
	 }
	     
	 public static void main(String[] args) throws Exception {

	    Configuration conf = new Configuration();
	    Job job = new Job(conf, "relativeFrequency");
	    job.setJarByClass(RelFreq.class);
	    job.setOutputKeyClass(Pair.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.waitForCompletion(true);
	 }

}