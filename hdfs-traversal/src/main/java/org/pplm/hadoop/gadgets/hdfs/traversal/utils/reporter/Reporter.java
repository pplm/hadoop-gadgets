package org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter;

import java.io.Closeable;
import java.io.Flushable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.pplm.hadoop.gadgets.hdfs.traversal.utils.ReflectUtils;

/**
 * 生成报告的基类
 * @author OracleGao
 *
 */
public abstract class Reporter implements Closeable, Flushable {
	
	private List<HeaderItem> headerItemList;
	
	public Reporter() {
		super();
	}
	
	public void init() throws ReportException {
		List<String> header = new ArrayList<>(headerSize());
		for (HeaderItem headerItem : headerItemList) {
			header.add(headerItem.getName());
		}
		if (header.size() > 0) {
			initHeader(header);
		}
	}
	
	public void setHeader(Class<? extends IReportable> reportableClass) {
		headerItemList = new ArrayList<HeaderItem>();
		HeaderItem headerItem = null;
		for (Field field : ReflectUtils.ReflectDeclaredFields(reportableClass)) {
			ReportHeaderItem reportHeaderItem = field.getAnnotation(ReportHeaderItem.class);
			if (reportHeaderItem != null) {
				headerItem = new HeaderItem();
				headerItem.setName(reportHeaderItem.name());
				headerItem.setDefaultValue(reportHeaderItem.defaultValue());
				headerItem.setFieldName(field.getName());
				headerItemList.add(headerItem);
			}
		}
	}

	public void addHeaderItem(HeaderItem headerItem) {
		if (headerItemList == null) {
			headerItemList = new ArrayList<HeaderItem>();
		}
		headerItemList.add(headerItem);
	}
	
	public void reportRecord(IReportable recordBean) throws ReportException {
		reportRecord(reflectRecord(recordBean));
	}
	
	protected abstract void initHeader(List<String> header) throws ReportException;
	
	public abstract void reportRecord(List<String> record) throws ReportException;
	
	private List<String> reflectRecord(IReportable recordBean) throws ReportException {
		List<String> record = new ArrayList<String>(headerSize());
		List<Field> fieldList = null;
		Object object = null;
		Field field = null;
		try {
			fieldList = ReflectUtils.ReflectDeclaredFields(recordBean.getClass());
			for (HeaderItem headerItem : headerItemList) {
				field = ReflectUtils.GetField(fieldList, headerItem.getFieldName());
				field.setAccessible(true);
				object = field.get(recordBean);
				record.add(object == null ? headerItem.getDefaultValue() : object.toString());
			}
		} catch (SecurityException | IllegalArgumentException | IllegalAccessException e) {
			 throw new ReportException(e);
		}
		return record;
	}

	public int headerSize() {
		return headerItemList.size();
	}
	
	public List<HeaderItem> getHeader() {
		return headerItemList;
	}

	public void setHeader(List<HeaderItem> header) {
		this.headerItemList = header;
	}

	public class HeaderItem {
		private String name;
		private String fieldName;
		private String defaultValue;
		
		public HeaderItem() {
			super();
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getFieldName() {
			return fieldName;
		}

		public void setFieldName(String fieldName) {
			this.fieldName = fieldName;
		}

		public String getDefaultValue() {
			return defaultValue;
		}

		public void setDefaultValue(String defaultValue) {
			this.defaultValue = defaultValue;
		}
	}
	
}
