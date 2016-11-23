package org.pplm.hadoop.gadgets.hdfs.traversal.repeat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsProcessMrJob;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HadoopConfig;
import org.pplm.hadoop.gadgets.mrjob.RepeatProcessMr;
import org.pplm.hadoop.gadgets.mrjob.RepeatProcessMr.Counter;
import org.pplm.hadoop.gadgets.mrjob.RepeatProcessMr.GlobalSortMapper;
import org.pplm.hadoop.gadgets.mrjob.RepeatProcessMr.GlobalSortPartitioner;
import org.pplm.hadoop.gadgets.mrjob.RepeatProcessMr.GlobalSortReducer;
import org.pplm.hadoop.gadgets.mrjob.RepeatProcessMr.RecordWritable;
import org.pplm.hadoop.gadgets.mrjob.RepeatProcessMr.RepeatProcessMapper;
import org.pplm.hadoop.gadgets.mrjob.RepeatProcessMr.RepeatProcessReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepeatProcessMrJob extends HdfsProcessMrJob {

	private Path outputPathFinal;
	
	public RepeatProcessMrJob(FileSystem fileSystem, Path inputPath, Path outputPath, Path outputPathFinal, Configuration config) {
		super(fileSystem, inputPath, outputPath, config);
		this.outputPathFinal = outputPathFinal;
	}

	@Override
	public void initJob(Job job) throws Exception {
		job.setJobName(RepeatProcessMrJob.class.getSimpleName() + ":" + inputPath.toString());
		job.setJarByClass(RepeatProcessMr.class);
		
		job.setMapperClass(RepeatProcessMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(RecordWritable.class);
		
		job.setReducerClass(RepeatProcessReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	}
	
	@Override
	public void afterJob(Job job) throws Exception {
		if (job.getJobState() == JobStatus.State.SUCCEEDED) {
			Counters counters = job.getCounters();
			super.putResult(Counter.TOTAL.value, counters.findCounter(Counter.TOTAL).getValue());
			super.putResult(Counter.REPEAT.value, counters.findCounter(Counter.REPEAT).getValue());
			super.putResult(Counter.REPEAT_TOTAL.value, counters.findCounter(Counter.REPEAT_TOTAL).getValue());
			super.putResult(Counter.SPACE.value, counters.findCounter(Counter.SPACE).getValue());
			if (config.getBoolean(RepeatProcessConfig.KEY_MR_STATISTICS_ONLY, RepeatProcessConfig.DEFAULT_MR_STATISTICS_ONLY)) {
				super.putResult(HadoopConfig.KEY_MR_RESULT_SUCCESS, true);
				fileSystem.delete(super.outputPath, true);
			} else {
				HdfsProcessMrJob hdfsProcessMrJob = new GlobalSortMrJob(fileSystem, super.outputPath, super.outputPathRoot, config);
				hdfsProcessMrJob.setResultProcess(new ResultCombine(fileSystem, outputPathFinal));
				hdfsProcessMrJob.run();
				//System.out.println("combine result:" + hdfsProcessMrJob.getBooleanResult(HadoopConfig.KEY_MR_RESULT_SUCCESS));
				super.putResult(HadoopConfig.KEY_MR_RESULT_SUCCESS, hdfsProcessMrJob.getBooleanResult(HadoopConfig.KEY_MR_RESULT_SUCCESS));
			}
		} else {
			super.putResult(HadoopConfig.KEY_MR_RESULT_SUCCESS, false);
		}
	}

	public static class GlobalSortMrJob extends HdfsProcessMrJob {

		public GlobalSortMrJob(FileSystem fileSystem, Path inputPath, Path outputPath, Configuration config) {
			super(fileSystem, inputPath, outputPath, config);
		}

		@Override
		public void initJob(Job job) throws Exception {
			job.setJobName(GlobalSortMrJob.class.getSimpleName() + ":" + inputPath.toString());
			job.setJarByClass(RepeatProcessMr.class);
			
			job.setMapperClass(GlobalSortMapper.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setPartitionerClass(GlobalSortPartitioner.class);
			
			job.setReducerClass(GlobalSortReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
		}

		@Override
		public void afterJob(Job job) throws Exception {
			if (job.getJobState() == JobStatus.State.SUCCEEDED) {
				super.putResult(HadoopConfig.KEY_MR_RESULT_SUCCESS, true);
			} else {
				super.putResult(HadoopConfig.KEY_MR_RESULT_SUCCESS, false);
			}
		}
		
	}
	
	public static class ResultCombine implements ResultProcess {
		private static Logger logger = LoggerFactory.getLogger(ResultCombine.class);
		
		private FileSystem fileSystem;
		private Path outputPath;
		
		public ResultCombine(FileSystem fileSystem, Path outputPath) {
			super();
			this.fileSystem = fileSystem;
			this.outputPath = outputPath;
		}
		
		@Override
		public void process(Path[] files) {
			InputStream inputStream = null;
			OutputStream outputStream = null;
			Arrays.sort(files);
			try {
				outputStream = fileSystem.create(outputPath);
				for (Path file : files) {
					try {
						inputStream = fileSystem.open(file);
						IOUtils.copyLarge(inputStream, outputStream);
					} finally {
						if (inputStream != null) {
							inputStream.close();
							inputStream = null;
						}
					}
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			} finally {
				if (outputStream != null) {
					try {
						outputStream.flush();
						outputStream.close();
					} catch (IOException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		}
	}
	
}
