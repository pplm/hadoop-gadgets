package org.pplm.hadoop.gadgets.schema.key;
/**
 * 
 * @author OracleGao
 *
 */
public class StringKey extends ValueKey {

	private static final long serialVersionUID = 1L;

	/**
	 * 字符串长度
	 */
	private int precision;

	public StringKey() {
		super(DataType.STRING);
	}
	
	public StringKey(int precision) {
		super(DataType.STRING);
	}

	@Override
	public int validate(String value) {
		if (precision > 0 && value.length() > precision) {
			return ValueKey.INVALD_PRECISION;
		}
		return ValueKey.VALID;
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

}
