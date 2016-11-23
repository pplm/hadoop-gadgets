package org.pplm.hadoop.gadgets.hdfs.traversal;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils.OutputFileUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HadoopConfig;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class HdfsProcessMrJob extends HdfsProcessJob {

	private static Logger logger = LoggerFactory.getLogger(HdfsProcessMrJob.class);
	
	protected Configuration config;
	
	private Job job;
	
	private ResultProcess resultProcess;
	
	private int reduceTaskMin = 0;
	
	protected Path outputPathRoot;
	
	public HdfsProcessMrJob(FileSystem fileSystem, Path inputPath, Path outputPath, Configuration config) {
		super(fileSystem, inputPath, new Path(outputPath, Utils.GenId()));
		this.config = config;
		this.reduceTaskMin = config.getInt(HadoopConfig.KEY_REDUCE_TASK_MIN, HadoopConfig.DEFAULT_REDUCE_TASK_MIN);
		this.outputPathRoot = outputPath;
	}

	@Override
	public void init() throws Exception {
		job = Job.getInstance(config);
		job.setJar(Thread.currentThread().getContextClassLoader().getResource(HadoopConfig.DEFAULT_MR_JOB_JAR).getPath());
		long fileLength = fileSystem.getFileLinkStatus(inputPath).getLen();
		int numReduceTask = (int) (fileLength / 1024 / 1024 / 1024);
		if (numReduceTask <= 1) {
			numReduceTask = reduceTaskMin;
		}
		job.setNumReduceTasks(numReduceTask);
		setInputOutput(job);
	}

	protected void setInputOutput(Job job) throws IOException {
		FileInputFormat.addInputPath(job, inputPath);
		fileSystem.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
	}
	
	@Override
	public void process() throws Exception {
		long time = System.currentTimeMillis();
		logger.info("begin to process");
		initJob(job);
		job.waitForCompletion(true);
		afterJob(job);
		if (resultProcess != null) {
			resultProcess.process(FileUtil.stat2Paths(fileSystem.listStatus(getMrOutputPath(), new OutputFileUtils.OutputFilesFilter())));
		}
		logger.info("mapreduce output path: [" + getMrOutputPath() + "]");
		if (config.getBoolean(HadoopConfig.KEY_MR_TEMP_DELETE, HadoopConfig.DEFAULT_MR_TEMP_DELETE)) {
			fileSystem.delete(getMrOutputPath(), true);
		}
		logger.info("finish to process, consume time[" + (System.currentTimeMillis() - time) + "] ms");
	}
	public abstract void initJob(Job job) throws Exception;
	public abstract void afterJob(Job job) throws Exception;
	
	@Override
	public void destory() {}

	public Path getMrOutputPath() {
		return FileOutputFormat.getOutputPath(job);
	}
	
	protected void addCacheFile(String file) {
		job.addCacheFile(new Path(file).toUri());
	}
	
	protected void addCacheFile(Path path) {
		job.addCacheFile(path.toUri());
	}
	
	public Configuration getConfig() {
		return config;
	}

	public void setConfig(Configuration config) {
		this.config = config;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}
	
	public ResultProcess getResultProcess() {
		return resultProcess;
	}

	public void setResultProcess(ResultProcess resultProcess) {
		this.resultProcess = resultProcess;
	}

	public static interface ResultProcess {
		public void process(Path[] files);
	}

	public int getReduceTaskMin() {
		return reduceTaskMin;
	}

	public void setReduceTaskMin(int reduceTaskMin) {
		this.reduceTaskMin = reduceTaskMin;
	}

	public Path getOutputPathRoot() {
		return outputPathRoot;
	}

	public void setOutputPathRoot(Path outputPathRoot) {
		this.outputPathRoot = outputPathRoot;
	}
	
}
