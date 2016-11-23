package com.k2data.bdpe.mr;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.k2data.bdpe.mr.SchemaProcessMr.SchemaProcessMapper;
import com.k2data.bdpe.mr.SchemaProcessMr.SchemaProcessReducer;

public class SchemaProcessMrTest {
	
	@Test
	public void main() throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.addCacheFile(new URI("/tmp/ecc.csv"));
		job.setJar("f:/mrjob.jar");
		job.setJobName("test");
		job.setJarByClass(SchemaProcessMr.class);
		job.setMapperClass(SchemaProcessMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// 设置partition
		// job.setPartitionerClass(NamePartitioner.class);
		// 在分区之后按照指定的条件分组
		// job.setGroupingComparatorClass(GroupComparator.class);

		job.setReducerClass(SchemaProcessReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
 
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.130.21:9000/user/fit/gaojz/input/schema.txt"));
		FileSystem.get(conf).delete(new Path("hdfs://192.168.130.21:9000/user/fit/gaojz/output/test"), true);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.130.21:9000/user/fit/gaojz/output/test"));
		job.waitForCompletion(true);
	}
}
