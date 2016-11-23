package org.pplm.hadoop.gadgets.mrjob.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;
import org.pplm.hadoop.gadgets.mrjob.RepeatProcessMr;
import org.pplm.hadoop.gadgets.mrjob.RepeatProcessMr.RepeatProcessMapper;
import org.pplm.hadoop.gadgets.mrjob.RepeatProcessMr.RepeatProcessReducer;

public class RepeatProcessMrTest {
	@Test
	public void main() throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJar("f:/mrjob.jar");
		job.setJobName("Job name:" + "test");
		  job.setJarByClass(RepeatProcessMr.class);
		  job.setMapperClass(RepeatProcessMapper.class);
		  job.setMapOutputKeyClass(BytesWritable.class);
		  job.setMapOutputValueClass(Text.class);
		  // 设置partition
		  //job.setPartitionerClass(NamePartitioner.class);
		  // 在分区之后按照指定的条件分组
		  //job.setGroupingComparatorClass(GroupComparator.class);

		  job.setReducerClass(RepeatProcessReducer.class);
		  job.setInputFormatClass(NLineInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(LongWritable.class);
		  job.setNumReduceTasks(1);
		  
		  FileInputFormat.addInputPath(job, new Path("hdfs://192.168.130.21:9000/user/fit/gaojz/input"));
		  FileSystem.get(conf).delete(new Path("hdfs://192.168.130.21:9000/user/fit/gaojz/output/test"), true);
		  FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.130.21:9000/user/fit/gaojz/output/test"));
		  FileOutputFormat.setCompressOutput(job, true);  //job使用压缩  
          FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		//  job.submit();
		  //State state = job.getJobState();
		  //job.getStatus().isJobComplete();
		  //do {
		  //System.out.print(state + ",");
		  //} while(true);
		  System.out.println("job start: " + job.waitForCompletion(true));
		  Counters counters = job.getCounters();
		  Counter totalCounter = counters.findCounter(RepeatProcessMr.Counter.TOTAL);
		  Counter repeatTotalCounter = counters.findCounter(RepeatProcessMr.Counter.REPEAT_TOTAL);
		  Counter repeatCounter = counters.findCounter(RepeatProcessMr.Counter.REPEAT);
		  System.out.println("total: " + totalCounter.getValue());
		  System.out.println("repeat total: " + repeatTotalCounter.getValue());
		  System.out.println("repeat: " + repeatCounter.getValue());
		
	}
}
