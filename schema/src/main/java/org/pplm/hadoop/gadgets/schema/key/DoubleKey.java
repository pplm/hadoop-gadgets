package org.pplm.hadoop.gadgets.schema.key;

import org.apache.commons.lang.StringUtils;

/**
 * 双精度，单精浮点型统一使用，双精度不支持无符号
 * @author OracleGao
 *
 */
public class DoubleKey extends NumericKey {
	
	private static final long serialVersionUID = 1L;

	private double min;
	private double max;
	
	private int precision;
	
	public DoubleKey(boolean unsigned) {
		super(unsigned ? DataType.UNSIGNED_FLOAT : DataType.FLOAT, unsigned);
	}
	
	@Override
	public void parseLeft(String value) throws KeyParseException {
		try {
			min = Double.parseDouble(value);
		} catch (Exception e) {
			throw new KeyParseException(e);
		}
	}

	@Override
	public void parseRight(String value) throws KeyParseException {
		try {
			max = Double.parseDouble(value);
		} catch (Exception e) {
			throw new KeyParseException(e);
		}
	}

	@Override
	public int validate(String value) {
		int result = ValueKey.VALID;
		try {
			double d = Double.parseDouble(value);
			if (d < 0 && super.unsigned) {
				result |= ValueKey.INVALD_SIGNED;
			}
			if (StringUtils.isNotBlank(left)) {
				if (d < min || (!leftEqual && d == min)) {
					result |= ValueKey.INVALD_LESS_THAN_MIN;
				}
			}
			if (StringUtils.isNotBlank(right)) {
				if (d > max || (!rightEqual && d == max)) {
					result |= ValueKey.INVALD_GREAT_THAN_MAX;
				}
			}
			if (precision > 0) {
				String s = StringUtils.substringAfter(value, ".");
				if (!value.equals(s)) {
					if (s.length() > precision) {
						result |= ValueKey.INVALD_PRECISION;
					}
				}
			}
		} catch (Exception e) {
			result |= ValueKey.INVALD_DATA_TYPE;
		}
		return result;
	}

	public double getMin() {
		return min;
	}

	public double getMax() {
		return max;
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}
	
}
