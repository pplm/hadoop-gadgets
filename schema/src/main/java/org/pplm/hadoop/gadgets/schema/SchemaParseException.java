package org.pplm.hadoop.gadgets.schema;

public class SchemaParseException extends Exception {

	private static final long serialVersionUID = 1L;

	public SchemaParseException() {
		super();
	}

	public SchemaParseException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public SchemaParseException(String message, Throwable cause) {
		super(message, cause);
	}

	public SchemaParseException(String message) {
		super(message);
	}

	public SchemaParseException(Throwable cause) {
		super(cause);
	}

}
