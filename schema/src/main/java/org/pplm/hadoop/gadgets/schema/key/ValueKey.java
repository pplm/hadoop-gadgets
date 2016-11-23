package org.pplm.hadoop.gadgets.schema.key;

import java.io.Serializable;

/**
 * 
 * @author OracleGao
 *
 */
public abstract class ValueKey implements Serializable {
	
	private static final long serialVersionUID = 1L;

	public static final int VALID = 0x00;
	public static final int INVALD_DATA_TYPE = 0x01;
	
	/**
	 * 目前精度判断采用宽松方式：当精度为2是，100.0,100,100.00均为有效精度值，而100.000,100.001,100.0000为无效精度值。
	 */
	public static final int INVALD_PRECISION = 0x02;
	public static final int INVALD_LESS_THAN_MIN = 0x04;
	public static final int INVALD_GREAT_THAN_MAX = 0x08;
	public static final int INVALD_SIGNED = 0x10;
	
	public static final int DATA_TYPE_INT = 0;
	public static final int DATA_TYPE_UNSIGNED_INT = 1;
	public static final int DATA_TYPE_BOOLEAN = 2;
	public static final int DATA_TYPE_STRING = 3;
	public static final int DATA_TYPE_FLOAT = 4;
	public static final int DATA_TYPE_UNSIGNED_FLOAT = 5;
	
	public enum DataType {
		INT(0),
		UNSIGNED_INT(1),
		BOOLEAN(2),
		STRING(3),
		FLOAT(4),
		UNSIGNED_FLOAT(5);
		
		public int value;
		
		DataType(int value) {
			this.value = value;
		}
	}
	
	private String name;
	/**
	 * 数据类型
	 * 0：整数、1：无符号整数、2：布尔（开关）、3：字符、4：浮点数、5：无符号浮点数
	 */
	private DataType dataType;
	
	public ValueKey(DataType dataType) {
		super();
		this.dataType = dataType;
	}

	public abstract int validate(String value);
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public DataType getDataType() {
		return dataType;
	}
	
}
