package org.pplm.hadoop.gadgets.hdfs.traversal;

import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Utils;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.IReportable;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.ReportHeaderItem;

public class HdfsProcessBean implements IReportable {
	@ReportHeaderItem(name = "name")
	private String name;
	@ReportHeaderItem(name = "path")
	private String path;
	@ReportHeaderItem(name = "uri")
	private String uri;
	@ReportHeaderItem(name = "length")
	private long length;
	@ReportHeaderItem(name = "readable length")
	private String readableLength;
	@ReportHeaderItem(name = "success")
	private boolean success;
	@ReportHeaderItem(name = "consume(s)")
	private long consume;

	public HdfsProcessBean() {
		super();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
		this.readableLength = Utils.ReadableFileLength(length);
	}

	public String getReadableLength() {
		return readableLength;
	}

	public void setReadableLength(String readableLength) {
		this.readableLength = readableLength;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public long getConsume() {
		return consume;
	}

	public void setConsume(long consume) {
		this.consume = consume;
	}
	
}
