package org.pplm.hadoop.gadgets.hdfs.extraction.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.pplm.hadoop.gadgets.hdfs.extraction.ExtractionConfig;
import org.pplm.hadoop.gadgets.hdfs.extraction.HdfsExtraction;
import org.pplm.hadoop.gadgets.hdfs.extraction.IHdfsExtraction;

public class MainTest {

	@Test
	public void mainTest() throws IOException {
		//temporary solution for setting HADOOP_USER_NAME
		ExtractionConfig.getInstance();
		
		FileSystem fileSystem = FileSystem.newInstance(new Configuration());
		IHdfsExtraction hdfsExtraction = new HdfsExtraction(fileSystem);
		hdfsExtraction.extracting("/tmp", "d:/tmp/test");
	}
	
}
