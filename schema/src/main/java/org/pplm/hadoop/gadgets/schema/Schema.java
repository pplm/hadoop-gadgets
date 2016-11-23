package org.pplm.hadoop.gadgets.schema;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * 
 * @author OracleGao
 *
 */
public class Schema {
	private Set<String> keySet = new HashSet<String>();
	
	private int hash;
	
	public Schema() {
		super();
	}

	public void combine(Schema schema) {
		keySet.addAll(schema.keySet);
	}
	
	public void addKey(String key) {
		keySet.add(key);
	}
	
	public void removeKey(String key) {
		keySet.remove(key);
	}

	public boolean containKey(String key) {
		return keySet.contains(key);
	}
	public int size() {
		return keySet.size();
	}
	
	public Collection<String> keys() {
		return keySet;
	}
	
	public boolean equals(Schema schema) {
		return this.hashCode() == schema.hashCode();
	}
	
	@Override
	public int hashCode() {
        int h = hash;
        if (h == 0 && keySet.size() > 0) {
            for (String key: keySet) {
                h = 31 * h + key.hashCode();
            }
            hash = h;
        }
        return h;
	}
	
	@Override
	public String toString() {
		return StringUtils.join(keySet, ',');
	}
	
	public String toString(String separator) {
		return StringUtils.join(keySet, separator);
	}

}
