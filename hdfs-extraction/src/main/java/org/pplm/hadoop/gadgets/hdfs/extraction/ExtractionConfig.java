package org.pplm.hadoop.gadgets.hdfs.extraction;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS 抽取配置
 * @author OracleGao
 */
public class ExtractionConfig {
	
	private Logger logger = LoggerFactory.getLogger(ExtractionConfig.class);
	
	private static String CONFIG_FILE = ExtractionConfig.class.getClassLoader().getResource("config.properties").getFile();
	
	private static String KEY_RECURSION = "hdfs.extraction.recursion";
	private static String KEY_FILTER_STRING = "hdfs.extraction.filter";
	private static String KEY_DIR_FILTERING = "hdfs.extraction.dir.filtering";
	private static String KEY_LINE_COUNT = "hdfs.extraction.lineCount";
	private static String KEY_CHARSET = "hdfs.extraction.charset";
	
	/**
	 * set into system properties
	 */
	private static String KEY_HADOOP_USER_NAME = "HADOOP_USER_NAME";
	
	private boolean recursion;
	private String filterStr;
	private boolean dirFiltering;
	private int lineCount;
	private String charset;
	private String hadoopUserName;
	
	private static final ExtractionConfig config = new ExtractionConfig();
	
	private ExtractionConfig() {
		super();
		init();
	}
	
	public static ExtractionConfig getInstance() {
		return config;
	}

	private void init() {
		Reader reader = null;
		try {
			reader = new FileReader(CONFIG_FILE);
			Properties properties = new Properties();
			properties.load(reader);
			this.recursion = Boolean.parseBoolean(properties.getProperty(KEY_RECURSION, "false"));
			this.filterStr = properties.getProperty(KEY_FILTER_STRING);
			this.dirFiltering = Boolean.parseBoolean(properties.getProperty(KEY_DIR_FILTERING, "false"));
			try {
				this.lineCount = Integer.parseInt(properties.getProperty(KEY_LINE_COUNT, "100"));
			} catch (Exception e) {
				this.lineCount = 100;
			}
			this.charset = properties.getProperty(KEY_CHARSET, "UTF-8");
			hadoopUserName = System.getenv(KEY_HADOOP_USER_NAME);
			if (hadoopUserName == null) {
				hadoopUserName = System.getProperty(KEY_HADOOP_USER_NAME, null);
			}
			if (hadoopUserName == null) {
				hadoopUserName = properties.getProperty(KEY_HADOOP_USER_NAME, null);
				if (hadoopUserName != null) {
					System.setProperty(KEY_HADOOP_USER_NAME, hadoopUserName);
				}
			}
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
				reader = null;
			} 
		}
	}
	
	public boolean isRecursion() {
		return recursion;
	}

	public void setRecursion(boolean recursion) {
		this.recursion = recursion;
	}

	public String getFilterStr() {
		return filterStr;
	}

	public void setFilterStr(String filterStr) {
		this.filterStr = filterStr;
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
	
	public boolean isDirFiltering() {
		return dirFiltering;
	}

	public void setDirFiltering(boolean dirFiltering) {
		this.dirFiltering = dirFiltering;
	}

	public String getHadoopUserName() {
		return hadoopUserName;
	}

	public void setHadoopUserName(String hadoopUserName) {
		this.hadoopUserName = hadoopUserName;
	}

	public FileExtractionConfig getFileExtractionConfig() {
		FileExtractionConfig fileExtractionConfig = new FileExtractionConfig();
		fileExtractionConfig = new FileExtractionConfig();
		if (lineCount > 0) {
			fileExtractionConfig.setLineCount(lineCount);
		}
		if (charset != null) {
			fileExtractionConfig.setCharset(charset);
		}
		return fileExtractionConfig;
	}
	
}
