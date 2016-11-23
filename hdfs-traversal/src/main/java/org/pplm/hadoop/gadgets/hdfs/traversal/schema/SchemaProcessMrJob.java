package org.pplm.hadoop.gadgets.hdfs.traversal.schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsProcessMrJob;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HadoopConfig;
import org.pplm.hadoop.gadgets.mrjob.SchemaProcessMr;
import org.pplm.hadoop.gadgets.mrjob.SchemaProcessMr.Counter;
import org.pplm.hadoop.gadgets.mrjob.SchemaProcessMr.SchemaProcessMapper;
import org.pplm.hadoop.gadgets.mrjob.SchemaProcessMr.SchemaProcessReducer;

public class SchemaProcessMrJob extends HdfsProcessMrJob {

	
		
	public SchemaProcessMrJob(FileSystem fileSystem, Path inputPath, Path outputPath, Configuration config) {
		super(fileSystem, inputPath, outputPath, config);
	}

	@Override
	public void initJob(Job job) throws Exception {
		job.setJobName(SchemaProcessMrJob.class.getSimpleName() + ":" + inputPath.toString());
		job.setJarByClass(SchemaProcessMr.class);
		job.setMapperClass(SchemaProcessMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(SchemaProcessReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	}

	@Override
	public void afterJob(Job job) throws Exception {
		if (job.getJobState() == JobStatus.State.SUCCEEDED) {
			Counters counters = job.getCounters();
			super.putResult(Counter.LINE_TOTAL.value, counters.findCounter(Counter.LINE_TOTAL).getValue());
			super.putResult(Counter.LINE_SPACE.value, counters.findCounter(Counter.LINE_SPACE).getValue());
			super.putResult(Counter.LINE_INVALID.value, counters.findCounter(Counter.LINE_INVALID).getValue());
			super.putResult(Counter.SCHEMA.value, counters.findCounter(Counter.SCHEMA).getValue());
			super.putResult(Counter.SCHEMA_MOST.value, counters.findCounter(Counter.SCHEMA_MOST).getValue());
			super.putResult(HadoopConfig.KEY_MR_RESULT_SUCCESS, true);
		} else {
			super.putResult(HadoopConfig.KEY_MR_RESULT_SUCCESS, false);
		}
	}

}
