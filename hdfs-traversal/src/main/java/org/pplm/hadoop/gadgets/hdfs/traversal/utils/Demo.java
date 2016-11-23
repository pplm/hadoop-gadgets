package org.pplm.hadoop.gadgets.hdfs.traversal.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Demo {

	public static void main(String[] args) throws IllegalArgumentException, IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		//fs.copyFromLocalFile(new Path("/sslconfig.properties"), new Path("/tmp/."));
		FSDataOutputStream fSDataOutputStream = fs.create(new Path("/tmp/test.log"));
		InputStream inputStream = new ByteArrayInputStream("1234567".getBytes());
		IOUtils.copyLarge(inputStream, fSDataOutputStream);
		fSDataOutputStream.close();
	}

}
