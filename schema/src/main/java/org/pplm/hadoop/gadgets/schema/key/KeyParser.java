package org.pplm.hadoop.gadgets.schema.key;

public abstract class KeyParser {
	public abstract ValueKey parse(String record) throws KeyParseException;
}
