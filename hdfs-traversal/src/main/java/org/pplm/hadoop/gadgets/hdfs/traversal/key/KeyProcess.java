package org.pplm.hadoop.gadgets.hdfs.traversal.key;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsOperater;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsTraversal;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Constant;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HdfsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyProcess	extends HdfsOperater {

	private Logger logger = LoggerFactory.getLogger(KeyProcess.class);
	
	public static String DEFAULT_OUTPUT_FILE = "keyProcess.csv";
	
	private Set<String> keySet = new HashSet<String>();
	
	public KeyProcess() {
		super("keyProcess", null);
	}

	@Override
	protected void init(FileSystem fileSystem) throws Exception {
		logger.info("key process begin");
		super.initConsume("keyProcess");
	}

	@Override
	protected void destory(FileSystem fileSystem) {
		OutputStream outputStream = null;
		try {
			fileSystem.mkdirs(super.outputPath);
			Path path = new Path(super.outputPath, DEFAULT_OUTPUT_FILE);
			outputStream = fileSystem.create(path, true);
			IOUtils.writeLines(keySet, IOUtils.LINE_SEPARATOR, outputStream, Constant.DEFAULT_ENCODING);
			outputStream.flush();
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (outputStream != null) {
				try {
					outputStream.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		logger.info("key process finish, consume [" + super.getConsume("keyProcess") + "] ms");
	}

	@Override
	protected void beforeDir(FileSystem fileSystem, Path path) {
		logger.info("key process begin in dir [" + path.toString() + "]");
	}

	@Override
	protected void afterDir(FileSystem fileSystem, Path path, int count, int fileCount, int dirCount) {
		logger.info("key process finish in dir [" + path.toString() + "]");
	}

	@Override
	protected void fileOperate(FileSystem fileSystem, Path path) {
		try {
			String line = HdfsUtils.ReadLine(fileSystem, path, Constant.DEFAULT_ENCODING, 2);
			if (line != null) {
				String[] keys = StringUtils.split(line);
				Collections.addAll(keySet, keys);
			}
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	public static void Process(String hdfsInputPath, String localOutputPath) throws Exception {
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.newInstance(new Configuration());
			HdfsOperater hdfsOperater = new KeyProcess();
			HdfsTraversal hdfsTraversal = new HdfsTraversal(fileSystem);
			hdfsTraversal.addListener(hdfsOperater);
			hdfsTraversal.traversal(hdfsInputPath);
			fileSystem.copyToLocalFile(hdfsOperater.getOutputPath(), new Path(localOutputPath));
		} finally {
			fileSystem.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		String hdfsInputPath = args[0];
		String localOutputPath = args[1];
		Process(hdfsInputPath, localOutputPath);
	}
	
}
