package org.pplm.hadoop.gadgets.schema;

import org.apache.commons.lang.StringUtils;

public class TextSchemaParser extends SchemaParser {
	
	private String itemSeparator = ",";
	private String keyValueSeparator = ":";
	
	public TextSchemaParser() {
		super();
	}
	
	/**
	 * 两种分隔符相同
	 * @param separator
	 */
	public TextSchemaParser(String separator) {
		super();
		this.itemSeparator = separator;
		this.keyValueSeparator = separator;
	}
	
	public TextSchemaParser(String itemSeparator, String keyValueSeparator) {
		super();
		this.itemSeparator = itemSeparator;
		this.keyValueSeparator = keyValueSeparator;
	}

	@Override
	public Schema parse(String record) throws SchemaParseException {
		System.out.println("item: " + itemSeparator + ", kv: " + keyValueSeparator);
		if (itemSeparator.equals(keyValueSeparator)) {
			return parse(record, itemSeparator);
		}
		return parse(record, itemSeparator, keyValueSeparator);
	}

	private Schema parse(String record, String separator) throws SchemaParseException {
		String[] items = StringUtils.split(record, separator);
		if (items.length % 2 != 0) {
			throw new SchemaParseException("key and value mismatch");
		}
		Schema schema = new Schema();
		for (int i = 0; i < items.length; i += 2) {
			schema.addKey(items[i]);
		}
		return schema;
	}
	
	/**
	 * 暂时没需求，不实现
	 * @param record
	 * @param itemSeparator
	 * @param keyValueSeparator
	 * @return
	 * @throws SchemaParseException
	 */
	private Schema parse(String record, String itemSeparator, String keyValueSeparator) throws SchemaParseException {
		return null;
	}
	
	public String getItemSeparator() {
		return itemSeparator;
	}

	public void setItemSeparator(String itemSeparator) {
		this.itemSeparator = itemSeparator;
	}

	public String getKeyValueSeparator() {
		return keyValueSeparator;
	}

	public void setKeyValueSeparator(String keyValueSeparator) {
		this.keyValueSeparator = keyValueSeparator;
	}

	
	@Override
	/**
	 * 使用itemSeparator分割
	 */
	public Schema parseKeys(String keys) {
		Schema schema = new Schema();
		String[] items = StringUtils.split(keys, itemSeparator);
		for (String key : items) {
			schema.addKey(key);
		}
		return schema;
	}

}
