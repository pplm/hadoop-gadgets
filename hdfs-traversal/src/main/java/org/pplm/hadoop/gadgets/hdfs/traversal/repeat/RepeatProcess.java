package org.pplm.hadoop.gadgets.hdfs.traversal.repeat;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsOperater;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsProcessMrJob;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsTraversal;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.FileBatcher;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HadoopConfig;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Utils;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.ReportException;
import org.pplm.hadoop.gadgets.mrjob.RepeatProcessMr.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepeatProcess extends HdfsOperater {
	private static Logger logger = LoggerFactory.getLogger(RepeatProcess.class);
	
	public RepeatProcess(RepeatProcessConfig config) {
		super("repeat", config);
	}

	@Override
	protected void init(FileSystem fileSystem) throws Exception {
		super.init(fileSystem);
		super.initReporter(fileSystem, RepeatProcessBean.REPORT_FILE_NAME, RepeatProcessBean.class);
	}

	@Override
	protected void destory(FileSystem fileSystem) throws IOException {
		try {
			super.closeReporter();
		} catch (ReportException | IOException e) {
			logger.error(e.getMessage(), e);
		}
		super.destory(fileSystem);
	}

	@Override
	protected void beforeDir(FileSystem fileSystem, Path path) {
		super.initConsume(path);
	}

	@Override
	protected void afterDir(FileSystem fileSystem, Path path, int count, int fileCount, int dirCount) {
		logger.info("directory [" + path + " ] consume time [" + super.getConsume(path) + "] ms");
	}

	@Override
	protected void fileOperate(FileSystem fileSystem, Path path) {
		long time = System.currentTimeMillis();
		RepeatProcessBean repeatProcessBean = new RepeatProcessBean();
		repeatProcessBean.setName(path.getName());
		repeatProcessBean.setPath(path.getParent().toUri().getRawPath());
		repeatProcessBean.setUri(path.toUri().toString());
		try {
			repeatProcessBean.setLength(fileSystem.getFileStatus(path).getLen());
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		HdfsProcessMrJob hdfsProcessMrJob = new RepeatProcessMrJob(fileSystem, path, super.getOutputPathTemp(path), super.getOutputPathResult(path), config);
		hdfsProcessMrJob.run();
		repeatProcessBean.setTotal(hdfsProcessMrJob.getLongResult(Counter.TOTAL.value));
		repeatProcessBean.setRepeat(hdfsProcessMrJob.getLongResult(Counter.REPEAT.value));
		repeatProcessBean.setRepeatTotal(hdfsProcessMrJob.getLongResult(Counter.REPEAT_TOTAL.value));
		repeatProcessBean.setSuccess(hdfsProcessMrJob.getBooleanResult(HadoopConfig.KEY_MR_RESULT_SUCCESS));
		repeatProcessBean.setConsume((System.currentTimeMillis() - time) / 1000);
		logger.info("repeat process output path [" + outputPath + "]");
		try {
			reporter.reportRecord(repeatProcessBean);
			reporter.flush();
		} catch (ReportException | IOException e) {
			logger.info(e.getMessage(), e);
		}
		logger.info("file [" + path + " ] consume time [" + (System.currentTimeMillis() - time) + "] ms");
	}
	
	public static void Process(String hdfsInputPath, String localOutputPath) throws Exception {
		FileSystem fileSystem = null;
		try {
			RepeatProcessConfig config = new RepeatProcessConfig();
			HdfsOperater hdfsOperater = new RepeatProcess(config);
			fileSystem = FileSystem.newInstance(config);
			HdfsTraversal hdfsTraversal = new HdfsTraversal(fileSystem);
			hdfsTraversal.addListener(hdfsOperater);
			hdfsTraversal.traversal(hdfsInputPath);
		} finally {
			if (fileSystem != null) {
				fileSystem.close();
			}
		}
	}

	public static void Process(List<String> hdfsInputPathList, String localOutputPath) throws Exception {
		FileSystem fileSystem = null;
		try {
			RepeatProcessConfig config = new RepeatProcessConfig();
			if (StringUtils.isNotBlank(localOutputPath)) {
				config.set(HadoopConfig.KEY_LOCAL_OUTPUT_PATH, localOutputPath);
			}
			HdfsOperater hdfsOperater = new RepeatProcess(config);
			fileSystem = FileSystem.newInstance(config);
			HdfsTraversal hdfsTraversal = new HdfsTraversal(fileSystem);
			hdfsTraversal.addListener(hdfsOperater);
			hdfsTraversal.traversal(hdfsInputPathList);
		} finally {
			if (fileSystem != null) {
				fileSystem.close();
			}
		}
	}
	
	public static void ProcessBatch(String file, String localOutputPath) throws Exception {
		String path = Utils.ResourcePath(file);
		FileBatcher fileBatcher = new FileBatcher(path);
		List<String> pathList = fileBatcher.getPathList();
		logger.info("repeat process list:");
		for (String temp : pathList) {
			logger.info(temp);
		}
		Process(pathList, localOutputPath);
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length == 2) {
			String hdfsInputPath = args[0];
			String localOutputPath = args[1];
			Process(hdfsInputPath, localOutputPath);
		} else if (args.length == 3) {
			if ("-f".equals(args[0])) {
				ProcessBatch(args[1], args[2]);
			}
		}
	}
	
}
