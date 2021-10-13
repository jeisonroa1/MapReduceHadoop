package BigDataProject1;

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
     

public class WordCountTest {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text words = new Text();
     
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
    StringTokenizer tokenizer = new StringTokenizer(line);
    while (tokenizer.hasMoreTokens()) {
        words.set(tokenizer.nextToken());
        context.write(words, one);
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
     
    Job teamjob = new Job(configurations, "wordcount");
    teamjob.setJarByClass(WordCountTest.class);
     
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


