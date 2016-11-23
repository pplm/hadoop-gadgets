package org.pplm.hadoop.gadgets.hdfs.traversal.schema;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsOperater;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsProcessMrJob;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsTraversal;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsProcessMrJob.ResultProcess;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Constant;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HadoopConfig;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.ReportException;
import org.pplm.hadoop.gadgets.mrjob.SchemaProcessMr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pplm.hadoop.gadgets.schema.SchemaManager;

public class SchemaProcess extends HdfsOperater {
	private static Logger logger = LoggerFactory.getLogger(SchemaProcess.class);
	
	public SchemaProcess(SchemaProcessConfig config) {
		super("schema", config);
	}
	
	@Override
	public void init(FileSystem fileSystem) throws Exception {
		super.init(fileSystem);
		super.initReporter(fileSystem, SchemaProcessBean.REPORT_FILE_NAME, SchemaProcessBean.class);
	}

	@Override
	public void destory(FileSystem fileSystem) {
		try {
			super.closeReporter();
		} catch (ReportException | IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public void beforeDir(FileSystem fileSystem, Path path) {
		super.initConsume(path);
	}

	@Override
	public void afterDir(FileSystem fileSystem, Path path, int count, int fileCount, int dirCount) {
		logger.info("directory [" + path + " ] consume time [" + super.getConsume(path) + "] ms");
	}

	@Override
	public void fileOperate(FileSystem fileSystem, Path path) {
		long time = System.currentTimeMillis();
		SchemaProcessBean schemaProcessBean = new SchemaProcessBean();
		schemaProcessBean.setName(path.getName());
		schemaProcessBean.setPath(path.getParent().toUri().getRawPath());
		schemaProcessBean.setUri(path.toUri().toString());
		boolean success = false;
		try {
			schemaProcessBean.setLength(fileSystem.getFileStatus(path).getLen());
			HdfsProcessMrJob hdfsProcessMrJob = new SchemaProcessMrJob(fileSystem, path, super.getOutputPathTemp(path), fileSystem.getConf());
			hdfsProcessMrJob.setResultProcess(new SchemaResultProcess(fileSystem, super.getOutputPathResult(path)));
			hdfsProcessMrJob.run();
			schemaProcessBean.setLineTotalCount(hdfsProcessMrJob.getLongResult(SchemaProcessMr.Counter.LINE_TOTAL.value));
			schemaProcessBean.setLineSpaceCount(hdfsProcessMrJob.getLongResult(SchemaProcessMr.Counter.LINE_SPACE.value));
			schemaProcessBean.setLineInvalidCount(hdfsProcessMrJob.getLongResult(SchemaProcessMr.Counter.LINE_INVALID.value));
			schemaProcessBean.setSchemaCount(hdfsProcessMrJob.getLongResult(SchemaProcessMr.Counter.SCHEMA.value));
			schemaProcessBean.setMostSchemaCount(hdfsProcessMrJob.getLongResult(SchemaProcessMr.Counter.SCHEMA_MOST.value));
			success = hdfsProcessMrJob.getBooleanResult(HadoopConfig.KEY_MR_RESULT_SUCCESS);
			logger.info("mapreduce output path [" + hdfsProcessMrJob.getMrOutputPath() + "]");
		} catch (Exception e) {
			success = false;
			logger.info(e.getMessage(), e);
		}
		logger.info("schema process output path [" + outputPath + "]");
		schemaProcessBean.setSuccess(success);
		schemaProcessBean.setConsume((System.currentTimeMillis() - time) / 1000);
		try {
			reporter.reportRecord(schemaProcessBean);
			reporter.flush();
		} catch (ReportException | IOException e) {
			logger.info(e.getMessage(), e);
		}
		logger.info("file [" + path + " ] consume time [" + (System.currentTimeMillis() - time) + "] ms");
	}

	private class SchemaResultProcess implements ResultProcess {
		private FileSystem fileSystem;
		private Path outputPath;
		private boolean allFlag;
		private String itemSeparator;

		public SchemaResultProcess(FileSystem fileSystem, Path outputPath) {
			super();
			this.fileSystem = fileSystem;
			this.outputPath = outputPath;
			this.allFlag = config.getBoolean(SchemaProcessConfig.KEY_FLAG_ALL, SchemaProcessConfig.DEFAULT_FLAG_ALL);
			this.itemSeparator = config.get(SchemaProcessConfig.KEY_SEPARATOR_ITEM, SchemaProcessConfig.DEFAULT_SEPARATOR_ITEM);
		}

		@Override
		public void process(Path[] files) {
			InputStream inputStream = null;
			SchemaManager schemaManager = new SchemaManager(allFlag);
			try {
				for (Path path : files) {
					try{
						inputStream = fileSystem.open(path);
						schemaManager.combine(SchemaManager.Parse(inputStream, Constant.DEFAULT_ENCODING, allFlag, itemSeparator, Constant.MR_OUTPUT_SEPARATOR));
					} finally {
						if (inputStream != null) {
							inputStream.close();
							inputStream = null;
						}
					}
				}
				OutputStream outputStream = null;
				try {
					outputStream = fileSystem.create(outputPath);
					schemaManager.mrSerialization(outputStream, Constant.DEFAULT_ENCODING, itemSeparator, Constant.MR_OUTPUT_SEPARATOR);
				} finally {
					if (outputStream != null) {
						outputStream.close();
					}
				}
			}  catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	public static void Process(String hdfsInputPath, String localOutputPath) throws Exception {
		FileSystem fileSystem = null;
		try {
			SchemaProcessConfig config = new SchemaProcessConfig();
			if (StringUtils.isNotBlank(localOutputPath)) {
				config.set(HadoopConfig.KEY_LOCAL_OUTPUT_PATH, localOutputPath);
			}
			HdfsOperater hdfsOperater = new SchemaProcess(config);
			fileSystem = FileSystem.newInstance(config);
			HdfsTraversal hdfsTraversal = new HdfsTraversal(fileSystem);
			hdfsTraversal.addListener(hdfsOperater);
			hdfsTraversal.traversal(hdfsInputPath);
			File file = new File(localOutputPath);
			file.mkdirs();
			fileSystem.copyToLocalFile(hdfsOperater.getOutputPath(), new Path(localOutputPath));
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
