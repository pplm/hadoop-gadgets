package org.pplm.hadoop.gadgets.schema.key;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvKeyParser extends KeyParser {
	private Logger logger = LoggerFactory.getLogger(CsvKeyParser.class);
	public static final String SEPARATOR = ",";
	public static final int PARAMETER_COUNT = 5;
	
	@Override
	public ValueKey parse(String record) throws KeyParseException {
		ValueKey key = null;
		try {
			if (StringUtils.isBlank(record)) {
				logger.warn("space record");
				return key;
			}
			String[] items = record.split(SEPARATOR, PARAMETER_COUNT);
			if (items.length < PARAMETER_COUNT) {
				throw new KeyParseException("invalid key record [" + record + "], item length [" + items.length + "] less than 5");
			}
			try {
				int dataType = Integer.parseInt(items[1]);
				switch(dataType) {
				case ValueKey.DATA_TYPE_INT:
					IntKey intKey = new IntKey(false);
					intKey.setLeft(items[2]);
					intKey.setRight(items[3]);
					key = intKey;
					break;
				case ValueKey.DATA_TYPE_UNSIGNED_INT:
					intKey = new IntKey(true);
					intKey.setLeft(items[2]);
					intKey.setRight(items[3]);
					key = intKey;
					break;
				case ValueKey.DATA_TYPE_BOOLEAN:
					key = new BooleanKey();
					break;
				case ValueKey.DATA_TYPE_STRING:
					if (StringUtils.isBlank(items[4])) {
						key = new StringKey(0);
					} else {
						key = new StringKey(Integer.parseInt(items[4]));
					}
					break;
				case ValueKey.DATA_TYPE_FLOAT:
					DoubleKey doubleKey = new DoubleKey(false);
					doubleKey.setLeft(items[2]);
					doubleKey.setRight(items[3]);
					if (StringUtils.isBlank(items[4])) {
						doubleKey.setPrecision(0);
					} else {
						doubleKey.setPrecision(Integer.parseInt(items[4]));
					}
					key = doubleKey;
					break;
				case ValueKey.DATA_TYPE_UNSIGNED_FLOAT:
					doubleKey = new DoubleKey(true);
					doubleKey.setLeft(items[2]);
					doubleKey.setRight(items[3]);
					if (StringUtils.isBlank(items[4])) {
						doubleKey.setPrecision(0);
					} else {
						doubleKey.setPrecision(Integer.parseInt(items[4]));
					}
					key = doubleKey;
					break;
				}
			} catch (Exception e) {
				throw new KeyParseException("key format exception [" + record + "]", e);
			}
			key.setName(items[0]);
		} catch (Exception e) {
			throw new KeyParseException(e);
		}
		return key;
	}

}
