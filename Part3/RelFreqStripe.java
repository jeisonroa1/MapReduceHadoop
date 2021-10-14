package BigDataProject;

import java.io.DataInput;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RelFreqStripe {
	
	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
	    private final static IntWritable one = new IntWritable(1);
	     
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String record = value.toString();
		    String[] words = record.split("\\s+");
		    for(int i=0; i<words.length-1; ++i){
		    	String u = words[i];
   			MapWritable tMap = new MapWritable();
		    	String[] window = Arrays.copyOfRange(words, i+1, words.length);
		    	for(String v : window){
		    		if(v.equals(u)) break;
   				Text vWrit = new Text(v);
   		    	if(!tMap.containsKey(vWrit)) tMap.put(vWrit, one);
   				else{
   				    int aux = ((IntWritable) tMap.get(vWrit)).get() + 1;
   				    tMap.put(vWrit, new IntWritable(aux));
   				}
		    	}
   			context.write(new Text(u), tMap);
		    }
	    }
	 }
	     
	public static class Reduce extends Reducer<Text, MapWritable, Text, PrintMapWritable> {
	
	    public void reduce(Text key, Iterable<MapWritable> values, Context context)
	      throws IOException, InterruptedException {
	        MapWritable finalMap = new MapWritable();
			for (MapWritable map : values) {
			    // Element-wise addition
				Iterator<MapWritable.Entry<Writable, Writable>> temporal = map.entrySet().iterator();
				while(temporal.hasNext()){
					MapWritable.Entry<Writable, Writable> entry = temporal.next();
					if(!finalMap.containsKey(entry.getKey())) finalMap.put(entry.getKey(), entry.getValue());
					else{
						int aux = ((IntWritable) finalMap.get(entry.getKey())).get();
						aux += ((IntWritable) entry.getValue()).get();
						finalMap.put(entry.getKey(), new IntWritable(aux));
					}
				}
			}
			//Sum every element in finalMap
			int s = 0; 
			Iterator<MapWritable.Entry<Writable, Writable>> temporal = finalMap.entrySet().iterator();
			while(temporal.hasNext()){
				MapWritable.Entry<Writable, Writable> entry = temporal.next();
				s += ((IntWritable) entry.getValue()).get();
			}
			//Emit relative frequency
			PrintMapWritable doubleMap = new PrintMapWritable();
			temporal = finalMap.entrySet().iterator();
			while(temporal.hasNext()){
				MapWritable.Entry<Writable, Writable> entry = temporal.next();
				String relFreq = ((IntWritable) entry.getValue()).get()+"/"+s;
				doubleMap.put((WritableComparable) entry.getKey(), new Text(relFreq));
			}
			context.write(key, doubleMap);
	    }
    }
	
	public static class PrintMapWritable extends SortedMapWritable {
	    @Override
	    public String toString() {
	        String map = "";
	        for(Entry<WritableComparable, Writable> entry : entrySet()) map += ", "+entry.getKey()+":"+entry.getValue();
	        return "{ "+map.substring(2)+" }";
	    }
	}
	     
    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
       Job job = new Job(conf, "relativeFrequency");
       job.setJarByClass(RelFreqStripe.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(PrintMapWritable.class);
       job.setMapOutputValueClass(MapWritable.class);
       job.setMapperClass(Map.class);
       job.setReducerClass(Reduce.class);
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
       job.waitForCompletion(true);
    }
}
