package org.pplm.hadoop.gadgets.hdfs.traversal;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsTraversalConfig {
	private static Logger logger = LoggerFactory.getLogger(HdfsTraversalConfig.class);
	
	private static String CONFIG_FILE_NAME = "hdfsTraversal.conf";
	
	private static String KEY_RECURSION = "bdpe.hdfs.traversal.recursion";
	private static String KEY_DIR_SKIPS = "bdpe.hdfs.traversal.dir.skips";
	private static String KEY_FILE_SKIPS = "bdpe.hdfs.traversal.file.skips";
	private static String KEY_OPERATERS = "bdpe.hdfs.traversal.operaters";
	
	private boolean recursion;
	private String[] dirSkips;
	private String[] fileSkips;
	private String[] operaters;
	
	public HdfsTraversalConfig() {
		super();
		init();
	}
	
	private void init() {
		try {
			PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_FILE_NAME);
			config.setListDelimiter(',');
			this.recursion = config.getBoolean(KEY_RECURSION, false);
			this.dirSkips = config.getStringArray(KEY_DIR_SKIPS);
			this.fileSkips = config.getStringArray(KEY_FILE_SKIPS);
			this.operaters = config.getStringArray(KEY_OPERATERS);
		} catch (ConfigurationException e) {
			logger.error(e.getMessage(), e);
		}
	}

	public boolean isRecursion() {
		return recursion;
	}

	public void setRecursion(boolean recursion) {
		this.recursion = recursion;
	}

	public String[] getDirSkips() {
		return dirSkips;
	}

	public void setDirSkips(String[] dirSkips) {
		this.dirSkips = dirSkips;
	}

	public String[] getFileSkips() {
		return fileSkips;
	}

	public void setFileSkips(String[] fileSkips) {
		this.fileSkips = fileSkips;
	}

	public String[] getOperaters() {
		return operaters;
	}

	public void setOperaters(String[] operaters) {
		this.operaters = operaters;
	}

}
