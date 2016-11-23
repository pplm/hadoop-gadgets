package org.pplm.hadoop.gadgets.hdfs.traversal.utils;

import java.io.File;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;

public class Utils {
	
	public static String ReadableFileLength(long length) {
		if (length <= 0) {
			return "0";
		} 
		double len = 1.0 * length;
		DecimalFormat decimalFormat = new DecimalFormat("#.0");
	
		double result = len / Constant.TB_SIZE;
		if (result > 1.0) {
			return String.valueOf(decimalFormat.format(result) + "T");
		}
		result = len / Constant.GB_SIZE;
		if (result > 1.0) {
			return String.valueOf(decimalFormat.format(result) + "G");
		}
		result = len / Constant.MB_SIZE;
		if (result > 1.0) {
			return String.valueOf(decimalFormat.format(result) + "M");
		}
		result = len / Constant.KB_SIZE;
		if (result > 1.0) {
			return String.valueOf(decimalFormat.format(result) + "K");
		}
		return String.valueOf(length + "B");
	}
	
	public static boolean isMatch(String src, String[] target) {
		for (String item : target) {
			if (src.matches(item)) {
				return true;
			}
		}
		return false;
	}
	
	public static String GenId() {
		return String.valueOf(new Date().getTime());
	}
	
	public static String same(String src1, String src2) {
		StringBuilder stringBuilder = new StringBuilder();
		int len1 = src1.length();
		int len2 = src2.length();
		int len = len1 < len2 ? len1 : len2;
		char c = 0;
		for (int i = 0; i < len; i++) {
			c = src1.charAt(i);
			if (c == src2.charAt(i)) {
				stringBuilder.append(c);
			} else {
				break;
			}
		}
		return stringBuilder.toString();
	}
	
	public static String same(List<String> stringList) {
		int size = stringList.size();
		if (size == 0) {
			return "";
		} else if (size == 1) {
			return stringList.get(0);
		} else if (size == 2) {
			return same(stringList.get(0), stringList.get(1));
		} else {
			String same = same(stringList.get(0), stringList.get(1));
			for (int i = 2; i < size; i++) {
				same = same(same, stringList.get(i));
			}
			return same;
		}
	}
	
	public static String samePath(List<String> pathList) {
		List<String> list = new ArrayList<String>();
		for (String path : pathList) {
			list.add(FilenameUtils.getFullPath(path));
		}
		return FilenameUtils.getFullPath(same(list));
	}
	
	public static String ResourcePath(String name) {
		URL url = Thread.currentThread().getContextClassLoader().getResource(name);
		if (url != null) {
			return url.getPath();
		}
		return name;
	}
	
	public static String getFileFullPath(String file) {
		String path = ResourcePath(file);
		File fileTmp = new File(path);
		if (fileTmp.exists()) {
			return path;
		}
		return null;
	}
	
	/**
	 * @param patternFlag {@link java.util.regex.Pattern}
	 * 
	 **/
	public static String replaceFirst(String str, String regex, String replacement, int patternFlag) {
		return Pattern.compile(regex, patternFlag).matcher(str).replaceFirst(replacement);
	}
}
