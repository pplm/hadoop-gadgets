package org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter;

import java.io.IOException;
import java.util.List;

public class NullReporter extends Reporter {

	@Override
	protected void initHeader(List<String> header) throws ReportException {}

	@Override
	public void reportRecord(List<String> record) throws ReportException {}

	@Override
	public void close() throws IOException {}

	@Override
	public void flush() throws IOException {}

}
