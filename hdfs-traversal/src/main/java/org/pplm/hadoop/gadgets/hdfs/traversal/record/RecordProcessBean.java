package org.pplm.hadoop.gadgets.hdfs.traversal.record;

import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsProcessBean;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.ReportHeaderItem;
/**
 * 
 * @author OracleGao
 *
 */
public class RecordProcessBean extends HdfsProcessBean {
	
	public static String REPORT_FILE_NAME = "recordReport.csv";
	
	@ReportHeaderItem(name = "total lines")
	private long totalLines;
	@ReportHeaderItem(name = "space lines")
	private long spaceLines;
	@ReportHeaderItem(name = "invalid lines")
	private long invalidLines;
	
	/**
	 * 存在无效key-value pair行的数量
	 */
	@ReportHeaderItem(name = "tagged lines")
	private long taggedLines;
	
	/**
	 * key-value pair中value总计数量
	 */
	@ReportHeaderItem(name = "pairs")
	private long pairs;
	
	/**
	 * key-value pair中不能匹配key的数量
	 */
	@ReportHeaderItem(name = "mismatchs")
	private long mismatchs;
	
	/**
	 * key-value pair中value有效数量
	 */
	@ReportHeaderItem(name = "valids")
	private long valids;
	/**
	 * key-value pair中value无效的数量
	 */
	@ReportHeaderItem(name = "invalids")
	private long invalids;
	/**
	 * key-value pair中value数据类型无效的数量
	 */
	@ReportHeaderItem(name = "invalid data types")
	private long invalidDataTypes;
	/**
	 * key-value pair中value小于最小值的数量
	 */
	@ReportHeaderItem(name = "lt mins")
	private long ltMins;
	/**
	 * key-value pair中value大于最大值的数量
	 */
	@ReportHeaderItem(name = "gt maxs")
	private long gtMaxs;
	/**
	 * key-value pair中value精度无效的数量
	 */
	@ReportHeaderItem(name = "invalid precisions")
	private long invalidPrecisions;
	/**
	 * key-value pair中value无效符号的数量
	 */
	@ReportHeaderItem(name = "invalid signs")
	private long invalidSigns;

	@ReportHeaderItem(name = "null values")
	private long nullValues;
	
	public RecordProcessBean() {
		super();
	}

	public long getSpaceLines() {
		return spaceLines;
	}

	public void setSpaceLines(long spaceLines) {
		this.spaceLines = spaceLines;
	}

	public long getInvalidLines() {
		return invalidLines;
	}

	public void setInvalidLines(long invalidLines) {
		this.invalidLines = invalidLines;
	}

	public long getTaggedLines() {
		return taggedLines;
	}

	public void setTaggedLines(long taggedLines) {
		this.taggedLines = taggedLines;
	}

	public long getPairs() {
		return pairs;
	}

	public void setPairs(long pairs) {
		this.pairs = pairs;
	}

	public long getMismatchs() {
		return mismatchs;
	}

	public void setMismatchs(long mismatchs) {
		this.mismatchs = mismatchs;
	}

	public long getValids() {
		return valids;
	}

	public void setValids(long valids) {
		this.valids = valids;
	}

	public long getInvalids() {
		return invalids;
	}

	public void setInvalids(long invalids) {
		this.invalids = invalids;
	}

	public long getInvalidDataTypes() {
		return invalidDataTypes;
	}

	public void setInvalidDataTypes(long invalidDataTypes) {
		this.invalidDataTypes = invalidDataTypes;
	}

	public long getLtMins() {
		return ltMins;
	}

	public void setLtMins(long ltMins) {
		this.ltMins = ltMins;
	}

	public long getGtMaxs() {
		return gtMaxs;
	}

	public void setGtMaxs(long gtMaxs) {
		this.gtMaxs = gtMaxs;
	}

	public long getInvalidPrecisions() {
		return invalidPrecisions;
	}

	public void setInvalidPrecisions(long invalidPrecisions) {
		this.invalidPrecisions = invalidPrecisions;
	}

	public long getInvalidSigns() {
		return invalidSigns;
	}

	public void setInvalidSigns(long invalidSigns) {
		this.invalidSigns = invalidSigns;
	}

	public long getTotalLines() {
		return totalLines;
	}

	public void setTotalLines(long totalLines) {
		this.totalLines = totalLines;
	}

	public long getNullValues() {
		return nullValues;
	}

	public void setNullValues(long nullValues) {
		this.nullValues = nullValues;
	}
	
}
