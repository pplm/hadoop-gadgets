package org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

public class HdfsCsvReporter extends CsvReporter {

	private FSDataOutputStream fsDataOutputStream;
	
	
	public HdfsCsvReporter() {
		super();
	}
/*
	public HdfsCsvReporter(FSDataOutputStream fsDataOutputStream, String encoding) {
		super(fsDataOutputStream, encoding);
		this.fsDataOutputStream = fsDataOutputStream;
	}

	public HdfsCsvReporter(FSDataOutputStream fsDataOutputStream) {
		super(fsDataOutputStream);
		this.fsDataOutputStream = fsDataOutputStream;
	}
	*/
	@Override
	public void flush() throws IOException {
		if (fsDataOutputStream != null) {
			fsDataOutputStream.flush();
			fsDataOutputStream.hflush();
			fsDataOutputStream.hsync();
		}
	}
	@Override
	public void close() throws IOException {
		if (fsDataOutputStream != null) {
			fsDataOutputStream.close();
		}
	}
	@Override
	protected void appendWrite(String line) throws IOException {}
	
}
