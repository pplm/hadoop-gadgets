package org.pplm.hadoop.gadgets.mrjob.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Utils.OutputFileUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GlobalSortMr {
	
	public static class GlobalSortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		@Override
		public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] pair = value.toString().split(context.getConfiguration().get(TextOutputFormat.SEPERATOR, "\t"));
			context.write(new LongWritable(Long.parseLong(pair[0])), new Text(pair[1]));
		}
		
	}
	
	public static class GlobalSortPartitioner extends Partitioner<LongWritable, Text> implements Configurable {

		private Logger logger = LoggerFactory.getLogger(GlobalSortPartitioner.class);
		
		private long length;
		
		private Configuration config;
		
		@Override
		public int getPartition(LongWritable key, Text value, int numPartitions) {
			if (length == 0) {
				throw new RuntimeException("length 0");
			}
			return ((int)(key.get() / (length / numPartitions + 1))) % numPartitions;
		}

		@Override
		public void setConf(Configuration conf) {
			config = conf;
			try {
				FileSystem fileSystem = FileSystem.get(conf);
				String pathsStr = conf.get(FileInputFormat.INPUT_DIR);
				String[] paths = StringUtils.split(pathsStr, StringUtils.COMMA);
				this.length = pathsLength(fileSystem, paths);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}

		@Override
		public Configuration getConf() {
			return config;
		}
		
		private long pathsLength(FileSystem fileSystem, String[] paths) throws IOException {
			long result = 0;
			Path path = null;
			for (String pathStr : paths) {
				path = new Path(pathStr);
				if (fileSystem.isFile(path)) {
					result += fileSystem.getFileStatus(path).getLen();
				}
				FileStatus[] fileStatuses = fileSystem.listStatus(path, new OutputFileUtils.OutputFilesFilter());
				for (FileStatus fileStatus : fileStatuses) {
					if (fileStatus.isFile()) {
						result += fileStatus.getLen();
					}
				}
			}
			return result;
		}
				 
	 }
	
	public static class GlobalSortReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, NullWritable.get());
			}
		}
	}
	
	public static void statup(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		job.setJar(Thread.currentThread().getContextClassLoader().getResource("mrjob.jar").getPath());
		job.setJarByClass(GlobalSortMr.class);
		
		job.setMapperClass(GlobalSortMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(GlobalSortPartitioner.class);
		
		job.setReducerClass(GlobalSortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(3);
		
		Path path = new Path(inputPath);
		FileInputFormat.addInputPath(job, path);
		path = new Path(outputPath);
		FileSystem.get(config).delete(path, true);
		FileOutputFormat.setOutputPath(job, path);
		job.waitForCompletion(true);
	}
	
}
