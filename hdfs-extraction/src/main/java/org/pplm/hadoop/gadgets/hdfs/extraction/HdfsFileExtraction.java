package org.pplm.hadoop.gadgets.hdfs.extraction;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HDFS 文件抽取
 * @author OracleGao
 *
 */
public class HdfsFileExtraction implements IHdfsFileExtraction {

	private FileSystem fileSystem;
	private FileExtractionConfig fileExtractionConfig;

	public HdfsFileExtraction(FileSystem fileSystem) {
		this(fileSystem, new FileExtractionConfig());
	}

	/**
	 * 
	 * @param fileSystem
	 * @param fileExtractionConfig
	 */
	public HdfsFileExtraction(FileSystem fileSystem, FileExtractionConfig fileExtractionConfig) {
		super();
		this.fileSystem = fileSystem;
		this.fileExtractionConfig = fileExtractionConfig;
	}
	
	/**
	 * 抽取文件内容
	 * @param hdfsFileSrc HDFS文件，如果非文件throw IOException
	 * @param dst dst如果是目录，则在该目录下创建hdfsFileSrc的同名文件，否则直接抽取。
	 * @throws IOException
	 */
	@Override
	public void fileExtracting(Path hdfsFileSrc, File dst) throws IOException {
		if (!fileSystem.isFile(hdfsFileSrc)) {
			throw new IOException("hdfsFileSrc is not a valid file on HDFS");
		}
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			inputStream = fileSystem.open(hdfsFileSrc);
			File file = dst;
			if (dst.isDirectory()) {
				file = new File(dst, hdfsFileSrc.getName());
			}
			outputStream = new FileOutputStream(file);
			IoUtils.copyLines(inputStream, outputStream, fileExtractionConfig.getCharset(), fileExtractionConfig.getLineCount());
		} catch (IOException e) {
			throw e;
		} finally {
			if (inputStream != null) {
				inputStream.close();
				inputStream = null;
			}
			if (outputStream != null) {
				outputStream.close();
				outputStream = null;
			}
		}
	}

}
