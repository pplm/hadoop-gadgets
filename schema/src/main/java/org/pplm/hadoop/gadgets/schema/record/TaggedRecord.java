package org.pplm.hadoop.gadgets.schema.record;

import java.util.ArrayList;
import java.util.List;
/**
 * 
 * @author OracleGao
 *
 */
public class TaggedRecord {
	public static final String SEPARATOR = "\t";
	
	private List<InvalidPair> invalidPairList = new ArrayList<InvalidPair>();
	
	private String record;
	
	public TaggedRecord(String record) {
		super();
		this.record = record;
	}
	
	public void addInvalid(String key, String value, int flag) {
		invalidPairList.add(new InvalidPair(key, value, flag));
	}
	
	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder(record);
		for (InvalidPair invalidPari : invalidPairList) {
			stringBuilder.append(SEPARATOR).append(invalidPari.toString());
		}
		return stringBuilder.toString();
	}
	
}
