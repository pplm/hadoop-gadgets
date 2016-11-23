package org.pplm.hadoop.gadgets.hdfs.traversal;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Constant;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HadoopConfig;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Utils;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.IReportable;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.LocalCsvReporter;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.ReportException;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.Reporter;

public abstract class HdfsOperater {

	public static String DEFAULT_OUTPUT_PATH = "/tmp/hdfsOperater";
	public static final String DEFAULT_OUTPUT_DIR = "hdfsOperater";
	public static final String DEFAULT_RESULT_DIR = "result";
	public static final String DEFAULT_TEMP_DIR = "temp";
	
	private Map<String, Long> consumeMap = new HashMap<String, Long>();
	
	protected HdfsTraversal hdfsTraversal;
	
	protected String name;
	protected Configuration config;

	protected String id;
	
	protected Path outputPath;
	protected Path outputPathResult;
	protected Path outputPathTemp;
	
	protected Reporter reporter;
	
	public HdfsOperater(String name, Configuration config) {
		super();
		this.name = name;
		this.config = config;
		this.id = Utils.GenId();
		this.outputPath = new Path(StringUtils.join(new Object[]{DEFAULT_OUTPUT_PATH, name, id}, IOUtils.DIR_SEPARATOR));
		this.outputPathResult = new Path(outputPath, DEFAULT_RESULT_DIR);
		this.outputPathTemp = new Path(outputPath, DEFAULT_TEMP_DIR);
	}

	protected void init(FileSystem fileSystem) throws Exception {
		fileSystem.mkdirs(outputPath);
		fileSystem.mkdirs(outputPathResult);
		fileSystem.mkdirs(outputPathTemp);
	}
	protected void destory(FileSystem fileSystem) throws IOException {
		if (config.getBoolean(HadoopConfig.KEY_MR_TEMP_DELETE, HadoopConfig.DEFAULT_MR_TEMP_DELETE)) {
			fileSystem.delete(outputPathTemp, true);
		}
	}
	
	protected abstract void beforeDir(FileSystem fileSystem, Path path);
	protected abstract void afterDir(FileSystem fileSystem, Path path, int count, int fileCount, int dirCount);
	
	protected abstract void fileOperate(FileSystem fileSystem, Path path);
	
	protected void initConsume(Path key) {
		initConsume(key.toString());
	}
	
	protected void initConsume(String key) {
		consumeMap.put(key, System.currentTimeMillis());
	}
	
	protected long getConsume(Path key) {
		return getConsume(key.toString());
	}
	
	protected long getConsume(String key) {
		return System.currentTimeMillis() - consumeMap.get(key);
	}
	
	protected void initReporter(FileSystem fileSystem, String fileName, Class<? extends IReportable> clazz) throws IOException, ReportException {
		File file = new File(StringUtils.join(new Object[]{config.get(HadoopConfig.KEY_LOCAL_OUTPUT_PATH, HadoopConfig.DEFAULT_LOCAL_OUTPUT_PATH), name, id}, IOUtils.DIR_SEPARATOR));
		file.mkdirs();
		this.reporter = new LocalCsvReporter(new File(file, fileName), Constant.DEFAULT_ENCODING);
		reporter.setHeader(clazz);
		reporter.init();
	}
	
	protected void closeReporter() throws ReportException, IOException {
		reporter.close();
	}
	
	public Path getOutputPathResult(Path path) {
		return new Path(outputPathResult, hdfsTraversal.getRelativePath(path));
	}
	
	public Path getOutputPathTemp(Path path) {
		return new Path(outputPathTemp, hdfsTraversal.getRelativePath(path));
	}
	
	public HdfsTraversal getHdfsTraversal() {
		return hdfsTraversal;
	}

	public void setHdfsTraversal(HdfsTraversal hdfsTraversal) {
		this.hdfsTraversal = hdfsTraversal;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Path getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(Path outputPath) {
		this.outputPath = outputPath;
	}

	public Path getOutputPathResult() {
		return outputPathResult;
	}

	public void setOutputPathResult(Path outputPathResult) {
		this.outputPathResult = outputPathResult;
	}

	public Path getOutputPathTemp() {
		return outputPathTemp;
	}

	public void setOutputPathTemp(Path outputPathTemp) {
		this.outputPathTemp = outputPathTemp;
	}
	
}
