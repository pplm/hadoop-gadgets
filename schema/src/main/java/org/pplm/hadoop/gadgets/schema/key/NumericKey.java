package org.pplm.hadoop.gadgets.schema.key;

import org.apache.commons.lang.StringUtils;

/**
 * 
 * @author OracleGao
 *
 */
public abstract class NumericKey extends ValueKey {

	private static final long serialVersionUID = 1L;

	protected String left;
	protected String right;
	
	protected boolean leftEqual;
	protected boolean rightEqual;
	
	protected String leftValue;
	protected String rightValue;
	
	protected boolean unsigned;
	
	public NumericKey(DataType dataType, boolean unsigned) {
		super(dataType);
		this.unsigned = unsigned;
	}

	public String getLeft() {
		return left;
	}

	public void setLeft(String left) throws KeyParseException {
		this.left = left;
		if (StringUtils.isNotBlank(left)) {
			char c = left.charAt(0);
			if (c == '(') {
				leftEqual = false;
				parseLeft(left.substring(1));
			} else {
				leftEqual = true;
				if (c == '[') {
					parseLeft(left.substring(1));
				} else {
					parseLeft(left);
				}
			}
		}
	}

	public abstract void parseLeft(String value) throws KeyParseException;
	
	public String getRight() {
		return right;
	}
	
	public void setRight(String right) throws KeyParseException {
		this.right = right;
		if (StringUtils.isNotBlank(right)) {
			int index = right.length() - 1;
			char c = right.charAt(index);
			if (c == ')') {
				rightEqual = false;
				parseRight(right.substring(0, index));
			} else {
				rightEqual = true;
				if (c == ']') {
					parseRight(right.substring(0, index));
				} else {
					parseRight(right);
				}
			}	
		}
	}
	
	public abstract void parseRight(String value) throws KeyParseException; 
	
	public boolean isLeftEqual() {
		return leftEqual;
	}

	public boolean isRightEqual() {
		return rightEqual;
	}

	public String getLeftValue() {
		return leftValue;
	}

	public String getRightValue() {
		return rightValue;
	}

	public void setRightValue(String rightValue) {
		this.rightValue = rightValue;
	}

	public boolean isUnsigned() {
		return unsigned;
	}

}
