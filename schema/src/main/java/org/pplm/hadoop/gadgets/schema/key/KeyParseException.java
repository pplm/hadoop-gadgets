package org.pplm.hadoop.gadgets.schema.key;

public class KeyParseException extends Exception {

	private static final long serialVersionUID = 1L;

	public KeyParseException() {
		super();
	}

	public KeyParseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public KeyParseException(String message, Throwable cause) {
		super(message, cause);
	}

	public KeyParseException(String message) {
		super(message);
	}

	public KeyParseException(Throwable cause) {
		super(cause);
	}
	
}
