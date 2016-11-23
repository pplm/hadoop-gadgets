package org.pplm.hadoop.gadgets.hdfs.extraction;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

/**
 * HDFS文件抽取接口
 * @author OracleGao
 *
 */
public interface IHdfsFileExtraction {
	/**
	 * 抽取文件内容
	 * @param hdfsFileSrc
	 * @param dst
	 * @throws IOException
	 */
	public void fileExtracting(Path hdfsFileSrc, File dst) throws IOException;
}
