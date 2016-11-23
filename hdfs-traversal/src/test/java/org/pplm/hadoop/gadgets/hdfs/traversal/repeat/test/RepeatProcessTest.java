package org.pplm.hadoop.gadgets.hdfs.traversal.repeat.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.pplm.hadoop.gadgets.hdfs.traversal.repeat.RepeatProcess;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Utils;


public class RepeatProcessTest {
	@Test
	public void main() throws Exception {
		RepeatProcess.Process("/user/piao/repeat", "f:/tmp/rp_output");
	}
	
	//@Test
	public void batch() throws Exception {
		//String aaa = StringUtils.substringAfter("hdfs://master01:9000/user/piao/repeat/r1", "hdfs://.*/");
		//System.out.println("[" + aaa + "]");
		
		//System.out.println(StringUtils.removeStart("hdfs://master01:9000/user/piao/repeat/r1", "hdfs://.*/"));
	//	System.out.println("/user/piao/repeat/r1".replaceFirst("hdfs://((?!/).)*/", "/"));
		//System.out.println(Utils.replaceFirst("/user/piao/repeat/r1", "hdfs://((?!/).)*/", "/", Pattern.CASE_INSENSITIVE));
	//	Matcher aaa = Pattern.compile("hdfs://((?!/).)*/", Pattern.CASE_INSENSITIVE).matcher(str).re;
		//System.out.println(aaa.replaceFirst("/"));
		
		/*
		if (str.startsWith("hdfs://")) {
			int i = str.indexOf("/", "hdfs://".length());
			System.out.println(str.substring(i));
		}*/
		
		RepeatProcess.ProcessBatch("repeat.file", "f:/tmp/rp_output");
	}
}
