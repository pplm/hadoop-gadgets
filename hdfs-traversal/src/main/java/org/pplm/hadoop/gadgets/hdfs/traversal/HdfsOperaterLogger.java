package org.pplm.hadoop.gadgets.hdfs.traversal;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsOperaterLogger extends HdfsOperater {

	private Logger logger = LoggerFactory.getLogger(HdfsOperaterLogger.class);
	
	public HdfsOperaterLogger() {
		super("logger", null);
	}

	@Override
	public void init(FileSystem fileSystem) {
		logger.info("hdfs operating begin");
	}

	@Override
	public void destory(FileSystem fileSystem) {
		logger.info("hdfs operating finish");
	}

	@Override
	public void beforeDir(FileSystem fileSystem, Path path) {
		logger.info("hdfs operating before directory [" + path.toString() + "]");
	}

	@Override
	public void afterDir(FileSystem fileSystem, Path path, int count, int fileCount, int dirCount) {
		logger.info("hdfs operating after directory [" + path.toString() + "], count [" + count + "], file count [" + fileCount + "], directory count [" + dirCount + "]");
	}
	
	@Override
	public void fileOperate(FileSystem fileSystem, Path path) {
		logger.info("traversal file [" + path.toString() + "]");
	}
	
}
