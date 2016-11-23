package org.pplm.hadoop.gadgets.hdfs.traversal.record;

import org.apache.commons.lang.StringUtils;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HadoopConfig;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Utils;

public class RecordProcessConfig extends HadoopConfig {
	public static final String CONFIG_FILE_NAME = "record-process.xml";
	
	public static final String KEY_SEPARATOR_ITEM = "bdpe.record.mr.separator.item";
	public static final String DEFAULT_SEPARATOR_ITEM = ",";
	
	public static final String KEY_SEPARATOR_KV = "bdpe.record.mr.separator.kv";
	public static final String DEFAULT_SEPARATOR_KV = ":";
	
	public static final String KEY_STATISTIC_ONLY = "bdpe.record.mr.statistics.only";
	public static final boolean DEFAULT_STATISTIC_ONLY = true;
	
	public static final String KEY_PARAM_FILE = "bdpe.record.mr.param.file";
	
	public static final String KEY_PARAM_FILE_HDFS = "bdpe.record.mr.param.file.hdfs";

	public static final String KEY_PARAM_FILES = "bdpe.record.mr.param.files";
	public static final String KEY_PARAM_FILES_MAPPING_PATTERN = "bdpe.record.mr.param.files.mapping.pattern";
	
	public RecordProcessConfig() {
		super(CONFIG_FILE_NAME);
		check();
	}
	
	private void check() {
		String file = super.get(KEY_PARAM_FILE);
		if (file == null) {
			throw new RuntimeException("parameter file [" + KEY_PARAM_FILE + "] is null");
		}
		String path = Utils.getFileFullPath(file);
		if (path == null) {
			throw new RuntimeException("parameter file [" + file + "] not found");
		}
		/*
		if (StringUtils.indexOfIgnoreCase(path, RecordProcessBean.REPORT_FILE_NAME) != -1 ) {
			throw new RuntimeException("parameter file [" + file + "] can not name " + RecordProcessBean.REPORT_FILE_NAME);
		}*/
		this.set(KEY_PARAM_FILE, path);
		String filesStr = super.get(KEY_PARAM_FILES);
		if (StringUtils.isBlank(filesStr)) {
			throw new RuntimeException("parameter files [" + KEY_PARAM_FILES + "] is null or empty");
		}
		String patternsStr = super.get(KEY_PARAM_FILES_MAPPING_PATTERN);
		if (StringUtils.isBlank(patternsStr)) {
			throw new RuntimeException("parameter files [" + KEY_PARAM_FILES_MAPPING_PATTERN + "] is null or empty");
		}
		String[] files = filesStr.split(",");
		String[] patterns = patternsStr.split(",");
		if (files.length != patterns.length) {
			throw new RuntimeException("parameter files and patterns mismatch");
		}
		StringBuilder stringBuilder = new StringBuilder(",");
		for (String temp : files) {
			path = Utils.getFileFullPath(temp);
			if (path == null) {
				throw new RuntimeException("parameter file [" + temp + "] not found");
			}
			stringBuilder.append(",").append(path);
		}
		stringBuilder.deleteCharAt(0);
		this.set(KEY_PARAM_FILES, stringBuilder.toString());
	}
	
}
