package org.pplm.hadoop.gadgets.hdfs.traversal.schema;

import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HadoopConfig;

public class SchemaProcessConfig extends HadoopConfig {

	public static final String CONFIG_FILE_NAME = "schema-process.xml";
	
	public static final String KEY_SEPARATOR_ITEM = "bdpe.mr.schema.separator.item";
	public static final String DEFAULT_SEPARATOR_ITEM = ",";
	
	public static final String KEY_SEPARATOR_KV = "bdpe.mr.schema.separator.kv";
	public static final String DEFAULT_SEPARATOR_KV = ":";
	
	public static final String KEY_FLAG_ALL = "bdpe.mr.schema.flag.all";
	public static final boolean DEFAULT_FLAG_ALL  = false;
		
	public SchemaProcessConfig() {
		super(CONFIG_FILE_NAME);
	}

}
