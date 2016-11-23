package org.pplm.hadoop.gadgets.hdfs.extraction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * extraction sample content of file which in hdfs
 */
public class Main {
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	
	public static void main(String[] args) throws IOException {
		logger.info("begin to extract");
		//temporary solution for setting HADOOP_USER_NAME
		ExtractionConfig.getInstance();
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.newInstance(new Configuration());
			IHdfsExtraction hdfsExtraction = new HdfsExtraction(fileSystem);
			hdfsExtraction.extracting(args[0], args[1]);
		} finally {
			if (fileSystem != null) {
				fileSystem.close();
				fileSystem = null;
			}
		}
		logger.info("extract finished");
	}

}
