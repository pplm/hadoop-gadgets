package com.k2data.bdpe.mr.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RepeatMr {
	public static class RepeatMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			System.out.println(key + ",");
			context.write(value, key);
		}
	}
	
	public static class RepeatReducer extends Reducer<Text, LongWritable, LongWritable, Text> {
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)  throws IOException, InterruptedException {
			long minOffset = -1;
			long temp = 0;
			for (LongWritable offset : values) {
				temp = offset.get();
				if (minOffset >= 0) {
					if (minOffset > temp) {
						minOffset = temp;
					}
				} else {
					minOffset = temp;
				}
			}
			context.write(new LongWritable(minOffset), key);
		}
	}
	
	public static void statup(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		job.setJar(Thread.currentThread().getContextClassLoader().getResource("mrjob.jar").getPath());
		job.setJarByClass(RepeatMr.class);
		
		job.setMapperClass(RepeatMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(RepeatReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		
		Path path = new Path(outputPath);
		FileSystem.get(config).delete(path, true);
		FileOutputFormat.setOutputPath(job, path);
		job.waitForCompletion(true);
	}
	
}
