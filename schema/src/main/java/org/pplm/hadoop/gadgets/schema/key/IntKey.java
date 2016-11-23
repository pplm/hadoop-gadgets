package org.pplm.hadoop.gadgets.schema.key;

import org.apache.commons.lang.StringUtils;

/**
 * 整型，长整型统一使用，长整型不支持无符号
 * @author OracleGao
 *
 */
public class IntKey extends NumericKey {

	private static final long serialVersionUID = 1L;
	
	private long min;
	private long max;
	
	public IntKey(boolean unsigned) {
		super(unsigned ? DataType.UNSIGNED_INT : DataType.INT, unsigned);
	}

	@Override
	public void parseLeft(String value) throws KeyParseException {
		try {
			min = Long.parseLong(value);
		} catch (Exception e) {
			throw new KeyParseException(e);
		}
	}

	@Override
	public void parseRight(String value) throws KeyParseException {
		try {
			max = Long.parseLong(value);
		} catch (Exception e) {
			throw new KeyParseException(e);
		}	
	}
	
	@Override
	public int validate(String value) {
		int result = ValueKey.VALID;
		try {
			long l = Long.parseLong(value);
			if (l < 0 && super.unsigned) {
				result |= ValueKey.INVALD_SIGNED;
			}
			if (StringUtils.isNotBlank(left)) {
				if (l < min || (!leftEqual && l == min)) {
					result |= ValueKey.INVALD_LESS_THAN_MIN;
				}
			}
			if (StringUtils.isNotBlank(right)) {
				if (l > max || (!rightEqual && l == max)) {
					result |= ValueKey.INVALD_GREAT_THAN_MAX;
				}
			}
		} catch (Exception e) {
			result |= ValueKey.INVALD_DATA_TYPE;
		}
		return result;
	}

	public long getMin() {
		return min;
	}

	public long getMax() {
		return max;
	}

}
