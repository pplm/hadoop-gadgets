package org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;

/**
 * @author OracleGao
 */
public abstract class CsvReporter extends Reporter {

	private static String LINE_SEPARATOR = IOUtils.LINE_SEPARATOR;
	private static String COLUMN_SEPARATOR = ",";
	
	public CsvReporter() {
		super();
	}

	@Override
	protected void initHeader(List<String> header) throws ReportException {
		try {
			appendWrite(getNewLine(header));
		} catch (IOException e) {
			throw new ReportException(e);
		}
	}

	@Override
	public void reportRecord(List<String> record) throws ReportException {
		try {
			synchronized (this) {
				appendWrite(getNewLine(record));
			}
		} catch (IOException e) {
			throw new ReportException(e);
		}
	}

	private String getNewLine(List<String> record) {
		StringBuilder line = new StringBuilder();
		for (String item : record) {
			line.append(item).append(COLUMN_SEPARATOR);
		}
		if (line.length() > 0) {
			line.deleteCharAt(line.length() - 1);
		}
		line.append(LINE_SEPARATOR);
		return line.toString();
	}
	
	protected abstract void appendWrite(String line) throws IOException;
	
}
