package org.pplm.hadoop.gadgets.hdfs.extraction;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS 目录和文件抽取
 * @author OracleGao
 */
public class HdfsExtraction implements IHdfsExtraction {

	private Logger logger = LoggerFactory.getLogger(HdfsExtraction.class);
	
	private ExtractionConfig extractionConfig;
	private FileSystem fileSystem;
	private IHdfsFileExtraction hdfsFileExtraction;
	private PathFilter pathFilter; 
	
	public HdfsExtraction(FileSystem hdfsFileSystem) {
		this(hdfsFileSystem, ExtractionConfig.getInstance());
	}
	
	public HdfsExtraction(FileSystem hdfsFileSystem, ExtractionConfig extractionConfig) {
		super();
		this.fileSystem = hdfsFileSystem;
		this.extractionConfig = extractionConfig;
		init();
	}

	private void init() {
		hdfsFileExtraction = new HdfsFileExtraction(fileSystem, extractionConfig.getFileExtractionConfig());
		if (extractionConfig.getFilterStr() != null) {
			this.pathFilter = new PathFilter() {
				private String filterStr = extractionConfig.getFilterStr();
				public boolean accept(Path path) {
					try {
						if (fileSystem.isDirectory(path) && !extractionConfig.isDirFiltering()){
							return true;
						}
					} catch (IOException e) {
						logger.error("never throw this exception.");
					}
					if (path.toString().matches(filterStr)) {
						return true;
					}
					return false;
				}
			};
		}
	}
	
	public void extracting(String hdfsPathSrc, String localPathDst) throws IOException {
		extracting(new Path(hdfsPathSrc), new File(localPathDst));
	}

	public void extracting(Path hdfsPathSrc, File localPathDst) throws IOException {
		if (!fileSystem.exists(hdfsPathSrc)) {
			throw new IOException("hdfsPathSrc [" + hdfsPathSrc.toString() + "] not exists on HDFS");
		}
		if (fileSystem.isFile(hdfsPathSrc)) {
			hdfsFileExtraction.fileExtracting(hdfsPathSrc, localPathDst);
		} else if (fileSystem.isDirectory(hdfsPathSrc)) {
			logger.info("extract directory [" + hdfsPathSrc.toString() + "]");
			if (!localPathDst.exists()) {
				localPathDst.mkdirs();
			} else if (!localPathDst.isDirectory()) {
				throw new IOException("localPathDst [" + localPathDst.getCanonicalPath() + "] is not a directory");
			}
			FileStatus[] fileStatuses = null;
			if (pathFilter != null) {
				fileStatuses = fileSystem.listStatus(hdfsPathSrc, pathFilter);
			} else {
				fileStatuses = fileSystem.listStatus(hdfsPathSrc);
			}
			Path path = null;
			for (FileStatus fileStatus : fileStatuses) {
				path = fileStatus.getPath();
				if (!extractionConfig.isRecursion() && fileSystem.isDirectory(path)) {
					continue;
				}
				extracting(path, new File(localPathDst, path.getName()));
			}
		}
	}
	
	public ExtractionConfig getExtractionConfig() {
		return extractionConfig;
	}

	public void setExtractionConfig(ExtractionConfig extractionConfig) {
		this.extractionConfig = extractionConfig;
	}

}
