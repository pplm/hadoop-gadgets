package org.pplm.hadoop.gadgets.hdfs.traversal.repeat;

import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HadoopConfig;

/**
 * 
 * @author OracleGao
 *
 */
public class RepeatProcessConfig extends HadoopConfig {
	
	public static final String CONFIG_FILE_NAME = "repeat-process.xml";
	
	public static String KEY_MR_SHA_ENABLE = "bdpe.mr.repeat.sha.enable";
	public static boolean DEFAULT_MR_SHA_ENABLE = true;
	
	public static String KEY_MR_STATISTICS_ONLY = "bdpe.mr.repeat.statistics.only";
	public static boolean DEFAULT_MR_STATISTICS_ONLY = true;
	
	public RepeatProcessConfig() {
		super(CONFIG_FILE_NAME);
	}
	
}
