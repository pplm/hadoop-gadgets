package org.pplm.hadoop.gadgets.hdfs.traversal.schema;

import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsProcessBean;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.ReportHeaderItem;

public class SchemaProcessBean extends HdfsProcessBean {
	
	public static String REPORT_FILE_NAME = "schemaReport.csv";

	@ReportHeaderItem(name = "total lines")
	private long lineTotalCount;
	@ReportHeaderItem(name = "space lines")
	private long lineSpaceCount;
	@ReportHeaderItem(name = "invalid lines")
	private long lineInvalidCount;
	@ReportHeaderItem(name = "schemas")
	private long schemaCount;
	@ReportHeaderItem(name = "most schemas")
	private long mostSchemaCount;

	public SchemaProcessBean() {
		super();
	}

	public long getLineTotalCount() {
		return lineTotalCount;
	}

	public void setLineTotalCount(long lineTotalCount) {
		this.lineTotalCount = lineTotalCount;
	}

	public long getLineSpaceCount() {
		return lineSpaceCount;
	}

	public void setLineSpaceCount(long lineSpaceCount) {
		this.lineSpaceCount = lineSpaceCount;
	}

	public long getLineInvalidCount() {
		return lineInvalidCount;
	}

	public void setLineInvalidCount(long lineInvalidCount) {
		this.lineInvalidCount = lineInvalidCount;
	}

	public long getSchemaCount() {
		return schemaCount;
	}

	public void setSchemaCount(long schemaCount) {
		this.schemaCount = schemaCount;
	}

	public long getMostSchemaCount() {
		return mostSchemaCount;
	}

	public void setMostSchemaCount(long mostSchemaCount) {
		this.mostSchemaCount = mostSchemaCount;
	}

}
