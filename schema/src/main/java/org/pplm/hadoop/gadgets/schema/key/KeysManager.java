package org.pplm.hadoop.gadgets.schema.key;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class KeysManager {
	public static final int KEY_MISMATCH = -1;
	public static final int VALUE_NULL = -2;
	
	private Map<String, ValueKey> keyMap = new HashMap<String, ValueKey>();

	public KeysManager() {
		super();
	}
	
	public int validate(String key, String value) {
		if (!keyMap.containsKey(key)) {
			return KEY_MISMATCH;
		}
		if (StringUtils.trimToNull(value) == null || "null".equalsIgnoreCase(StringUtils.trim(value))) {
			return VALUE_NULL;
		}
		return keyMap.get(key).validate(value);
	}
	
	public void initKey(List<String> recordList) throws KeyParseException {
		KeyParser keyParser = new CsvKeyParser();
		ValueKey key = null;
		for (String record : recordList) {
			key = keyParser.parse(record);
			if (key != null) {
				keyMap.put(key.getName(), key);
			}
			
		}
	}
	
	public Map<String, ValueKey> getKeyMap() {
		return keyMap;
	}

	public void setKeyMap(Map<String, ValueKey> keyMap) {
		this.keyMap = keyMap;
	}

}
