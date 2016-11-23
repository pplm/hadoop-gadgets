package org.pplm.hadoop.gadgets.schema.record;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author OracleGao
 *
 */
public class RecordParser {
	
	private String itemSeparator = ",";
	private String keyValueSeparator = ":";
	
	public RecordParser() {
		super();
	}
	
	/**
	 * 两种分隔符相同
	 * @param separator
	 */
	public RecordParser(String separator) {
		super();
		this.itemSeparator = separator;
		this.keyValueSeparator = separator;
	}
	
	public RecordParser(String itemSeparator, String keyValueSeparator) {
		super();
		this.itemSeparator = itemSeparator;
		this.keyValueSeparator = keyValueSeparator;
	}

	public Map<String, String> parse(String line) throws RecordParseException {
		if (itemSeparator.equals(keyValueSeparator)) {
			return parse(line, itemSeparator);
		}
		return null;
	}
	
	private Map<String, String> parse(String record, String separator) throws RecordParseException {
		String[] items = record.split(separator);
		if (items.length % 2 != 0) {
			throw new RecordParseException("invalid line");
		}
		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < items.length - 1; i += 2) {
			map.put(items[i], items[i + 1]);
		}
		return map;
	}
	
}
