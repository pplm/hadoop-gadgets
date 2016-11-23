package org.pplm.hadoop.gadgets.hdfs.traversal;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HdfsProcessJob implements Runnable {
	
	private static Logger logger = LoggerFactory.getLogger(HdfsProcessJob.class);
	
	public enum State {
		DEFINE(1), 
		INIT(2), 
		RUNNING(3),
		SUCCESS(4),
		FAILED(5);
		
		public int value;
		
		State(int value) {
			this.value = value;
		}
	}
	
	private State state = State.DEFINE; 
	
	protected FileSystem fileSystem;
	protected Path inputPath;
	protected Path outputPath;
	
	private Map<String, String> result = new HashMap<String, String>();
	
	public HdfsProcessJob (FileSystem fileSystem, Path inputPath, Path outputPath) {
		super();
		this.fileSystem = fileSystem;
		this.inputPath = inputPath;
		this.outputPath = outputPath;
	}
	
	protected abstract void init() throws Exception;
	protected abstract void process() throws Exception;
	protected abstract void destory();
	
	protected void putResult(String key, String value) {
		result.put(key, value);
	}
	
	protected void putResult(String key, boolean value) {
		result.put(key, String.valueOf(value));
	}
	
	protected void putResult(String key, int value) {
		result.put(key, String.valueOf(value));
	}
	
	protected void putResult(String key, long value) {
		result.put(key, String.valueOf(value));
	}

	public String getResult(String key) {
		return getResult(key, null);
	}
	
	public String getResult(String key, String defaultValue) {
		if (result.containsKey(key)) {
			return result.get(key);
		}
		return defaultValue;
	}
	
	public boolean getBooleanResult(String key) {
		return getBooleanResult(key, false);
	}
	
	public boolean getBooleanResult(String key, boolean defaultValue) {
		if (result.containsKey(key)) {
			return Boolean.parseBoolean(result.get(key));
		}
		return defaultValue;
	}
	
	public int getIntResult(String key) {
		return getIntResult(key, 0);
	}
	
	public int getIntResult(String key, int defaultValue) {
		if (result.containsKey(key)) {
			return Integer.parseInt(result.get(key));
		}
		return defaultValue;
	}
	
	public long getLongResult(String key) {
		return getLongResult(key, 0);
	}
	
	public long getLongResult(String key, long defaultValue) {
		if (result.containsKey(key)) {
			return Long.parseLong(result.get(key));
		}
		return defaultValue;
	}
	
	public Map<String, String> getResults() {
		return result;
	}
	
	@Override
	public void run() {
		try {
			state = State.INIT;
			init();
			state = State.RUNNING;
			process();
		} catch (Exception e) {
			state = State.FAILED;
			logger.error(e.getMessage(), e);
		} finally {
			destory();
			if (state != State.FAILED) {
				state = State.SUCCESS; 
			}
		}
	}

	public FileSystem getFileSystem() {
		return fileSystem;
	}

	public void setFileSystem(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	public Path getInputPath() {
		return inputPath;
	}

	public void setInputPath(Path inputPath) {
		this.inputPath = inputPath;
	}

	public Path getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(Path outputPath) {
		this.outputPath = outputPath;
	}

	public State getState() {
		return state;
	}

}
