package org.pplm.hadoop.gadgets.schema;

import org.apache.hadoop.conf.Configuration;

/**
 * 
 * @author OracleGao
 *
 */
public abstract class SchemaParser {
	
	public SchemaParser() {
		super();
	}
	
	public abstract Schema parse(String record) throws SchemaParseException;
	public abstract Schema parseKeys(String keys);
	
	public static SchemaParser GetParser(Configuration config) {
		return null;
	}
	
}
