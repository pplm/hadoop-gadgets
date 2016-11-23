package org.pplm.hadoop.gadgets.hdfs.extraction;

import java.io.IOException;

/**
 * HDFS抽取接口
 * @author OracleGao
 *
 */
public interface IHdfsExtraction {
	/**
	 * 
	 * @param hdfsPathSrc
	 * @param localPathDst
	 * @throws IOException
	 */
	public void extracting(String hdfsPathSrc, String localPathDst) throws IOException;
	
}
