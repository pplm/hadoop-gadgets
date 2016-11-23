package org.pplm.hadoop.gadgets.hdfs.traversal.record;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsOperater;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsProcessMrJob;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsTraversal;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsProcessMrJob.ResultProcess;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HadoopConfig;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.RegularHashMap;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.RegularMap;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.ReportException;
import org.pplm.hadoop.gadgets.mrjob.RecordProcessMr.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordProcess extends HdfsOperater {
	private Logger logger = LoggerFactory.getLogger(RecordProcess.class);
	
	private RegularMap<Path> regularMap = new RegularHashMap<Path>();
	
	public RecordProcess(RecordProcessConfig config) {
		super("record", config);
	}

	@Override
	protected void init(FileSystem fileSystem) throws Exception {
		super.init(fileSystem);
		super.initReporter(fileSystem, RecordProcessBean.REPORT_FILE_NAME, RecordProcessBean.class);
		String file = config.get(RecordProcessConfig.KEY_PARAM_FILE);
		Path path = new Path(file);
		fileSystem.copyFromLocalFile(path, super.outputPath);
		path = new Path(super.outputPath, path.getName());
		config.set(RecordProcessConfig.KEY_PARAM_FILE_HDFS, path.toString());
	}

	private void initParamFileMapping(FileSystem fileSystem) {
		String files = config.get(RecordProcessConfig.KEY_PARAM_FILES);
		String patterns = config.get(RecordProcessConfig.KEY_PARAM_FILES_MAPPING_PATTERN);
		
		
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
		logger.info("record process output path [" + outputPath + "]");
		RecordProcessBean recordProcessBean = new RecordProcessBean();
		recordProcessBean.setName(path.getName());
		recordProcessBean.setPath(path.getParent().toUri().getRawPath());
		recordProcessBean.setUri(path.toUri().toString());
		boolean success = false;
		try {
			recordProcessBean.setLength(fileSystem.getFileStatus(path).getLen());
			HdfsProcessMrJob hdfsProcessMrJob = new RecordProcessMrJob(fileSystem, path, super.getOutputPathTemp(path), fileSystem.getConf());
			hdfsProcessMrJob.setResultProcess(new ResultCombine(fileSystem, super.getOutputPathResult(path)));
			hdfsProcessMrJob.run();
			recordProcessBean.setGtMaxs(hdfsProcessMrJob.getLongResult(Counter.INVALID_GT_MAXS.value));
			recordProcessBean.setInvalidDataTypes(hdfsProcessMrJob.getLongResult(Counter.INVALID_DATA_TYPES.value));
			recordProcessBean.setInvalidLines(hdfsProcessMrJob.getLongResult(Counter.INVALID_LINES.value));
			recordProcessBean.setInvalidPrecisions(hdfsProcessMrJob.getLongResult(Counter.INVALID_PRECISIONS.value));
			recordProcessBean.setInvalids(hdfsProcessMrJob.getLongResult(Counter.INVALIDS.value));
			recordProcessBean.setInvalidSigns(hdfsProcessMrJob.getLongResult(Counter.INVALID_SIGNS.value));
			recordProcessBean.setLtMins(hdfsProcessMrJob.getLongResult(Counter.INVALID_LT_MINS.value));
			recordProcessBean.setMismatchs(hdfsProcessMrJob.getLongResult(Counter.MISMATCHS.value));
			recordProcessBean.setNullValues(hdfsProcessMrJob.getLongResult(Counter.NULL_VALUES.value));
			recordProcessBean.setPairs(hdfsProcessMrJob.getLongResult(Counter.PAIRS.value));
			recordProcessBean.setValids(hdfsProcessMrJob.getLongResult(Counter.VALIDS.value));
			recordProcessBean.setSpaceLines(hdfsProcessMrJob.getLongResult(Counter.SPACE_LINES.value));
			recordProcessBean.setTaggedLines(hdfsProcessMrJob.getLongResult(Counter.TAGGED_LINES.value));
			recordProcessBean.setTotalLines(hdfsProcessMrJob.getLongResult(Counter.TOTAL_LINES.value));
			success = hdfsProcessMrJob.getBooleanResult(HadoopConfig.KEY_MR_RESULT_SUCCESS);
		} catch (IOException e) {
			success = false;
			logger.error(e.getMessage(), e);
		}
		
		recordProcessBean.setSuccess(success);
		recordProcessBean.setConsume((System.currentTimeMillis() - time) / 1000);
		try {
			reporter.reportRecord(recordProcessBean);
			reporter.flush();
		} catch (ReportException | IOException e) {
			logger.info(e.getMessage(), e);
		}
		logger.info("file [" + path + " ] consume time [" + (System.currentTimeMillis() - time) + "] ms");
		
	}

	public static class ResultCombine implements ResultProcess {
		private static Logger logger = LoggerFactory.getLogger(ResultCombine.class);
		
		private FileSystem fileSystem;
		private Path outputPath;
		
		public ResultCombine(FileSystem fileSystem, Path outputPath) {
			super();
			this.fileSystem = fileSystem;
			this.outputPath = outputPath;
		}
		
		@Override
		public void process(Path[] files) {
			InputStream inputStream = null;
			OutputStream outputStream = null;
			Arrays.sort(files);
			try {
				outputStream = fileSystem.create(outputPath);
				for (Path file : files) {
					try {
						inputStream = fileSystem.open(file);
						IOUtils.copyLarge(inputStream, outputStream);
					} finally {
						if (inputStream != null) {
							inputStream.close();
							inputStream = null;
						}
					}
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			} finally {
				if (outputStream != null) {
					try {
						outputStream.flush();
						outputStream.close();
					} catch (IOException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		}
	}
	public static void Process(String hdfsInputPath, String localOutputPath) throws Exception {
		FileSystem fileSystem = null;
		try {
			RecordProcessConfig config = new RecordProcessConfig();
			if (StringUtils.isNotBlank(localOutputPath)) {
				config.set(HadoopConfig.KEY_LOCAL_OUTPUT_PATH, localOutputPath);
			}
			HdfsOperater hdfsOperater = new RecordProcess(config);
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
	
	public static void main(String[] args) throws Exception {
		String hdfsInputPath = args[0];
		String localOutputPath = args[1];
		Process(hdfsInputPath, localOutputPath);
	}
}
