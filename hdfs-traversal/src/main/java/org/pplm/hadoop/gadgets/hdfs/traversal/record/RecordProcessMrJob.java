package org.pplm.hadoop.gadgets.hdfs.traversal.record;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsProcessMrJob;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HadoopConfig;
import org.pplm.hadoop.gadgets.mrjob.RecordProcessMr;
import org.pplm.hadoop.gadgets.mrjob.RecordProcessMr.Counter;
import org.pplm.hadoop.gadgets.mrjob.RecordProcessMr.GlobalSortPartitioner;
import org.pplm.hadoop.gadgets.mrjob.RecordProcessMr.RecordProcessMapper;
import org.pplm.hadoop.gadgets.mrjob.RecordProcessMr.RecordProcessReducer;
/**
 * 
 * @author OracleGao
 *
 */
public class RecordProcessMrJob extends HdfsProcessMrJob {

	public RecordProcessMrJob(FileSystem fileSystem, Path inputPath, Path outputPath, Configuration config) {
		super(fileSystem, inputPath, outputPath, config);
	}

	@Override
	public void initJob(Job job) throws Exception {
		job.setJobName(RecordProcessMrJob.class.getSimpleName() + ":" + inputPath.toString());
		job.setJarByClass(RecordProcessMr.class);
		job.setMapperClass(RecordProcessMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(GlobalSortPartitioner.class);
		job.setReducerClass(RecordProcessReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		super.addCacheFile(config.get(RecordProcessConfig.KEY_PARAM_FILE_HDFS));
	}

	@Override
	public void afterJob(Job job) throws Exception {
		if (job.getJobState() == JobStatus.State.SUCCEEDED) {
			Counters counters = job.getCounters();
			super.putResult(Counter.TOTAL_LINES.value, counters.findCounter(Counter.TOTAL_LINES).getValue());
			super.putResult(Counter.INVALID_DATA_TYPES.value, counters.findCounter(Counter.INVALID_DATA_TYPES).getValue());
			super.putResult(Counter.INVALID_GT_MAXS.value, counters.findCounter(Counter.INVALID_GT_MAXS).getValue());
			super.putResult(Counter.INVALID_LINES.value, counters.findCounter(Counter.INVALID_LINES).getValue());
			super.putResult(Counter.INVALID_LT_MINS.value, counters.findCounter(Counter.INVALID_LT_MINS).getValue());
			super.putResult(Counter.INVALID_PRECISIONS.value, counters.findCounter(Counter.INVALID_PRECISIONS).getValue());
			super.putResult(Counter.INVALID_SIGNS.value, counters.findCounter(Counter.INVALID_SIGNS).getValue());
			super.putResult(Counter.INVALIDS.value, counters.findCounter(Counter.INVALIDS).getValue());
			super.putResult(Counter.MISMATCHS.value, counters.findCounter(Counter.MISMATCHS).getValue());
			super.putResult(Counter.NULL_VALUES.value, counters.findCounter(Counter.NULL_VALUES).getValue());
			super.putResult(Counter.PAIRS.value, counters.findCounter(Counter.PAIRS).getValue());
			super.putResult(Counter.SPACE_LINES.value, counters.findCounter(Counter.SPACE_LINES).getValue());
			super.putResult(Counter.TAGGED_LINES.value, counters.findCounter(Counter.TAGGED_LINES).getValue());
			super.putResult(Counter.VALIDS.value, counters.findCounter(Counter.VALIDS).getValue());
			super.putResult(HadoopConfig.KEY_MR_RESULT_SUCCESS, true);
		} else {
			super.putResult(HadoopConfig.KEY_MR_RESULT_SUCCESS, false);
		}
	}

}
