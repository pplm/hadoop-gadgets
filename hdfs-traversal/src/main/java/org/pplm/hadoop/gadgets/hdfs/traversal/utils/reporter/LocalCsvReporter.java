package org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

public class LocalCsvReporter extends CsvReporter {

	public static final String DEFAULT_ENCODING = "utf-8";
	
	private File file;
	private String encoding;
	private Writer writer;
	
	public LocalCsvReporter(File file) {
		this(file, DEFAULT_ENCODING);
	}

	public LocalCsvReporter(File file, String encoding) {
		super();
		this.file = file;
		this.encoding = encoding;
	}
	
	@Override
	public void init() throws ReportException {
		try {
			this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), encoding));
		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			throw new ReportException(e);
		}
		super.init();
	}
	
	@Override
	protected void appendWrite(String line) throws IOException {
		writer.write(line);
	}

	@Override
	public void flush() throws IOException {
		writer.flush();
	}

	@Override
	public void close() throws IOException {
		if (writer != null) {
			writer.close();
		}
	}

}
