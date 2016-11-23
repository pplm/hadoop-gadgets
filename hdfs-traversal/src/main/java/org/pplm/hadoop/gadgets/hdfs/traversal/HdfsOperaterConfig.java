package org.pplm.hadoop.gadgets.hdfs.traversal;

import org.apache.hadoop.conf.Configuration;

public class HdfsOperaterConfig {
	private String hdfsInputPath;
	private String hdfsOutputPath;
	private Configuration hadoopConfig;
	
	public HdfsOperaterConfig() {
		super();
	}

	public HdfsOperaterConfig(Configuration hadoopConfig) {
		super();
		this.hadoopConfig = hadoopConfig;
	}

	public HdfsOperaterConfig(String hdfsInputPath, String hdfsOutputPath, Configuration hadoopConfig) {
		super();
		this.hdfsInputPath = hdfsInputPath;
		this.hdfsOutputPath = hdfsOutputPath;
		this.hadoopConfig = hadoopConfig;
	}

	public String getHdfsInputPath() {
		return hdfsInputPath;
	}

	public void setHdfsInputPath(String hdfsInputPath) {
		this.hdfsInputPath = hdfsInputPath;
	}

	public String getHdfsOutputPath() {
		return hdfsOutputPath;
	}

	public void setHdfsOutputPath(String hdfsOutputPath) {
		this.hdfsOutputPath = hdfsOutputPath;
	}

	public Configuration getHadoopConfig() {
		return hadoopConfig;
	}

	public void setHadoopConfig(Configuration hadoopConfig) {
		this.hadoopConfig = hadoopConfig;
	}
	
}
