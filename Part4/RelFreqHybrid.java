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

public class RelFreqHybrid{
	
	public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable>{
		private java.util.Map<Pair, IntWritable> combiningMap;
		private final static IntWritable one = new IntWritable(1);
		
		public void setup(Context context){
			combiningMap = new HashMap<>();
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
		    String[] events = line.split("\\s+");
		    for(int i=0; i<events.length-1; ++i){
		    	String u = events[i];
		    	String[] window = Arrays.copyOfRange(events, i+1, events.length);
		    	for(String v : window){
		    		if(v.equals(u)) break;
					Pair uv = new Pair(new Text(u), new Text(v));
			    	if(!combiningMap.containsKey(uv)) combiningMap.put(uv, one);
					else{
					    int aux = ((IntWritable) combiningMap.get(uv)).get() + 1;
					    combiningMap.put(uv, new IntWritable(aux));
					}
		    	}
		    }
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			Iterator<java.util.Map.Entry<Pair, IntWritable>> temp = combiningMap.entrySet().iterator();
			while(temp.hasNext()){
				java.util.Map.Entry<Pair, IntWritable> entry = temp.next();
				context.write((Pair) entry.getKey(), (IntWritable) entry.getValue());
			}
		}
	}
	
	public static class Reduce extends Reducer<Pair, IntWritable, Text, PrintMapWritable>{
		private Text uPrev;
		private PrintMapWritable wmap;
		
		public void setup(Context context){
			uPrev = null;
			wmap = new PrintMapWritable();
		}
		
		public void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			Pair p = new Pair(key);
			if(uPrev != null && !p.getS1().toString().equals(uPrev.toString())){
				emit(context);
				wmap.clear();
			}
			//H{v} = sum(c1,c2,...);
			int sum = 0;
			for (IntWritable val : values) sum += val.get();
			if(!wmap.containsKey((Text) p.getS2()))
				wmap.put((Text) p.getS2(), new Text(String.valueOf(sum)));
			else{
				int aux = Integer.valueOf(((Text) wmap.get(p.getS2())).toString());
				aux += sum;
				wmap.put((Text) p.getS2(), new Text(String.valueOf(aux)));
			}
			uPrev = new Text(p.getS1());
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			emit(context);
		}
		
		public void emit(Context context) throws IOException, InterruptedException{
			//sum all elements in wmap
			int s = 0; 
			Iterator<Entry<WritableComparable, Writable>> temp = wmap.entrySet().iterator();
			while(temp.hasNext()){
				Entry<WritableComparable, Writable> entry = temp.next();
				s += Integer.valueOf(((Text) entry.getValue()).toString());
			}
			//emit relative frequency
			PrintMapWritable doubleMap = new PrintMapWritable();
			temp = wmap.entrySet().iterator();
			while(temp.hasNext()){
				Entry<WritableComparable, Writable> entry = temp.next();
				//double relFreq = Integer.valueOf(((Text) entry.getValue()).toString()) / ((double) s);
				//doubleMap.put((WritableComparable) entry.getKey(), new DoubleWritable(relFreq));
				String relFreq = ((Text) entry.getValue()).toString()+"/"+s;
				doubleMap.put((Text) entry.getKey(), new Text(relFreq));
			}
			context.write(uPrev, doubleMap);
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
	
	public static class Pair implements WritableComparable<Pair>{
		 public Text s1;
		 public Text s2;
		 public Pair(){
			 this.s1 = new Text();
			 this.s2 = new Text();
		 }
		 public Pair(Pair p){
			 this.s1 = p.getS1();
			 this.s2 = p.getS2();
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
			//return comp==0 ? this.s2.compareTo(p.s2) : comp;
		}
		@Override
		public boolean equals(Object o){
			if(this == o) return true;
			if(o == null || this.getClass() != o.getClass()) return false;
			Pair p = (Pair) o;
			if(this.s1 != null ? !this.s1.equals(p.s1) : p.s1 != null) return false;
			return this.s2 != null? this.s2.equals(p.s2) : p.s2 == null;
		}
		@Override
		public int hashCode(){
			int result = s1 != null ? s1.hashCode() : 0;
			result = 31 * result + (s2 !=null ? s2.hashCode() : 0);
			return result;
		}

	}
		 
	public static void main(String[] args) throws Exception{
	    Configuration conf = new Configuration();
	     
	    Job job = new Job(conf, "relativeFreq");
	    job.setJarByClass(RelFreqHybrid.class);
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(PrintMapWritable.class);
	    job.setMapOutputKeyClass(Pair.class);
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