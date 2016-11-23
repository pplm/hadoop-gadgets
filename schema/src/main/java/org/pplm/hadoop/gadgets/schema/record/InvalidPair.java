package org.pplm.hadoop.gadgets.schema.record;
/**
 * 
 * @author OracleGao
 *
 */
public class InvalidPair {
	
	public static final String SEPARATOR = "#";
	public static final String MISS_VALUE = "miss";
	
	private String key;
	private String value;
	private int flag;
	
	public InvalidPair() {
		super();
	}

	public InvalidPair(String key, String value, int flag) {
		super();
		this.key = key;
		this.value = value;
		this.flag = flag;
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(key).append(SEPARATOR).append(value).append(SEPARATOR);
		if (flag < 0) {
			stringBuilder.append(MISS_VALUE);
		} else {
			stringBuilder.append(Integer.toBinaryString(flag));
		}
		return stringBuilder.toString();
	}
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}
	
}
