package org.pplm.hadoop.gadgets.hdfs.traversal.utils;

import java.util.HashMap;
import java.util.Set;
import java.util.regex.Pattern;

public class RegularHashMap<V> extends HashMap<String, V> implements RegularMap<V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public V regularGet(String key) {
		Set<String> patternSet = super.keySet();
		for(String pattern : patternSet) {
			if (key.matches(pattern)) {
				return super.get(pattern);
			}
		}
		return null;
	}

	@Override
	public V regularGetIgnoreCase(String key) {
		Set<String> patternSet = super.keySet();
		for(String pattern : patternSet) {
			if (Pattern.compile(pattern, Pattern.CASE_INSENSITIVE).matcher(key).matches()) {
				return super.get(pattern);
			}
		}
		return null;
	}

}
