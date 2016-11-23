package org.pplm.hadoop.gadgets.schema.key;

/**
 * 
 * @author OracleGao
 *
 */
public class BooleanKey extends ValueKey {

	private static final long serialVersionUID = 1L;

	public BooleanKey() {
		super(DataType.BOOLEAN);
	}

	@Override
	public int validate(String value) {
		if ("0".equals(value) || "1".equals(value) || "false".equals(value) || "true".equals(value)) {
			return ValueKey.VALID;
		}
		return ValueKey.INVALD_DATA_TYPE;
	}

}
