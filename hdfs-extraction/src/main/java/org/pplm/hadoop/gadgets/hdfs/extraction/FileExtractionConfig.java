package org.pplm.hadoop.gadgets.hdfs.extraction;

/**
 * HDFS 文件抽取配置
 * @author OracleGao
 *
 */
public class FileExtractionConfig {
	private int lineCount = 100;
	private String charset = "UTF-8";
	
	public FileExtractionConfig() {
		super();
	}

	public int getLineCount() {
		return lineCount;
	}

	public void setLineCount(int lineCount) {
		this.lineCount = lineCount;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}
	
}
