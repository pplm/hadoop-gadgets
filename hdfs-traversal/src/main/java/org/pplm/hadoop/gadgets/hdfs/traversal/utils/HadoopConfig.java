package org.pplm.hadoop.gadgets.hdfs.traversal.utils;

import org.apache.hadoop.conf.Configuration;

public class HadoopConfig extends Configuration {
	
	public static final String KEY_REDUCE_TASK_MIN = "bdpe.mr.reduce.task.min";
	public static final int DEFAULT_REDUCE_TASK_MIN = 10;
	
	public static final String DEFAULT_MR_JOB_JAR = "mrjob.jar";
	public static final String KEY_MR_RESULT_SUCCESS = "bdpe.mr.job.result.success";
	
	public static final String KEY_MR_TEMP_DELETE = "bdpe.mr.temp.delete";
	public static final boolean DEFAULT_MR_TEMP_DELETE = false;
	
	public static final String KEY_LOCAL_OUTPUT_PATH = "bdpe.ht.local.output.path";
	public static final String DEFAULT_LOCAL_OUTPUT_PATH = "./output";
	
	private String[] configFiles;
	
	public HadoopConfig(String... configFiles) {
		super();
		this.configFiles = configFiles;
		init();
	}
	
	private void init() {
		for (String configFile : configFiles) {
			if (super.getResource(configFile) == null) {
				throw new RuntimeException("not found config file [" + configFile + "]");
			}
			super.addResource(configFile);
		}
	}

}
