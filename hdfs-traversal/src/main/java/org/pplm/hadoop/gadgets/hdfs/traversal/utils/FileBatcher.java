package org.pplm.hadoop.gadgets.hdfs.traversal.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

/**
 * 提取统计结果文件中需要批量处理的文件
 * @author OracleGao
 *
 */
public class FileBatcher {
	private String file;
	public FileBatcher(String file) {
		this.file = file;
	} 
	
	public List<String> getPathList() throws IOException {
		List<String> list = new ArrayList<String>();
		List<String> recordList = FileUtils.readLines(new File(file));
		for (String record : recordList) {
			if (StringUtils.isBlank(record)) {
				continue;
			}
			String[] items = record.split(",");
			if (items.length < 2) {
				continue;
			}
			if (Long.parseLong(items[0]) > 0) {
				list.add(Utils.replaceFirst(items[1], "hdfs://((?!/).)*/", "/", Pattern.CASE_INSENSITIVE));
			}
		}
		return list;
	}
	
}
