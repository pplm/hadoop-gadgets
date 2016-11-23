package org.pplm.hadoop.gadgets.hdfs.traversal.utils;

import java.util.Map;

public interface RegularMap<V> extends Map<String, V> {
	public V regularGet(String key);
	public V regularGetIgnoreCase(String key);
}
