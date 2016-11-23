package org.pplm.hadoop.gadgets.hdfs.traversal.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtils {
	
	public static List<String> ReadLines(FileSystem fileSystem, Path filePath, String encoding) throws IOException {
		if (!fileSystem.isFile(filePath)) {
			return null;
		}
		List<String> lineList = null;
		InputStream inputStream = null; 
		try {
			inputStream = fileSystem.open(filePath);
			lineList = IOUtils.readLines(inputStream, encoding);
		}  finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
		return lineList;
	}
	
	public static List<String> ReadFirstLines(FileSystem fileSystem, Path filePath, String encoding, long size) throws IOException {
		if (!fileSystem.isFile(filePath)) {
			return null;
		}
		List<String> lineList = new ArrayList<String>();
		if (size <= 0) {
			return lineList;
		}
		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath), encoding));
			String line = null;
			while((line = bufferedReader.readLine()) != null && size-- > 0) {
				lineList.add(line);
			}
		}  finally {
			if (bufferedReader != null) {
				bufferedReader.close();
			}
		}
		return lineList;
	}
	
	public static String ReadFirstLine(FileSystem fileSystem, Path filePath, String encoding) throws IOException {
		List<String> lineList = ReadFirstLines(fileSystem, filePath, encoding, 1);
		if (lineList != null && lineList.size() == 1) {
			return lineList.get(0);
		}
		return null;
	}
	
	public static String ReadLine(FileSystem fileSystem, Path filePath, String encoding, long rowNum) throws IOException {
		List<String> lineList = ReadFirstLines(fileSystem, filePath, encoding, rowNum);
		if (lineList.size() == rowNum) {
			return lineList.get(lineList.size() - 1);
		}
		return null;
	}
	
}
