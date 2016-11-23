package org.pplm.hadoop.gadgets.schema.record;
/**
 * 
 * @author OracleGao
 *
 */
public class RecordParseException extends Exception {

	private static final long serialVersionUID = 1L;

	public RecordParseException() {
		super();
	}

	public RecordParseException(String message, Throwable cause, boolean enableSuppression,	boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public RecordParseException(String message, Throwable cause) {
		super(message, cause);
	}

	public RecordParseException(String message) {
		super(message);
	}

	public RecordParseException(Throwable cause) {
		super(cause);
	}

}
