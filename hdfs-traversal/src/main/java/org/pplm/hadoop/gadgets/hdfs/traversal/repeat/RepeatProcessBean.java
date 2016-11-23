package org.pplm.hadoop.gadgets.hdfs.traversal.repeat;

import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsProcessBean;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.ReportHeaderItem;
/**
 * 
 * @author OracleGao
 *
 */
public class RepeatProcessBean extends HdfsProcessBean {
	
	public static String REPORT_FILE_NAME = "repeatReport.csv";
	
	@ReportHeaderItem(name = "total")
	private long total;
	@ReportHeaderItem(name = "repeat")
	private long repeat;
	@ReportHeaderItem(name = "repeat total")
	private long repeatTotal;
	@ReportHeaderItem(name = "space line")
	private long space;
	 
	public RepeatProcessBean() {
		super();
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public long getRepeat() {
		return repeat;
	}

	public void setRepeat(long repeat) {
		this.repeat = repeat;
	}

	public long getRepeatTotal() {
		return repeatTotal;
	}

	public void setRepeatTotal(long repeatTotal) {
		this.repeatTotal = repeatTotal;
	}

	public long getSpace() {
		return space;
	}

	public void setSpace(long space) {
		this.space = space;
	}

}
