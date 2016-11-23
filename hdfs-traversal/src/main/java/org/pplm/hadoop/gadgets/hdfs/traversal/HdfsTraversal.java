package org.pplm.hadoop.gadgets.hdfs.traversal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Listener;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 对HDFS文件系统遍历
 * @author OracleGao
 *
 */
public class HdfsTraversal extends Listener<HdfsOperater> {
	private Logger logger = LoggerFactory.getLogger(HdfsTraversal.class);
	
	private FileSystem fileSystem;
	
	private HdfsTraversalConfig config;
	
	private Path traversalRootPath;
	
	public HdfsTraversal() {
		super();
		config = new HdfsTraversalConfig();
		init();
	}

	public HdfsTraversal(FileSystem fileSystem) {
		this();
		this.fileSystem = fileSystem; 
	}
	
	private void init() { 
		String[] operaters = config.getOperaters();
		if (operaters != null) {
			for (String temp : operaters) {
				try {
					addListener(temp);
				} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}

	@Override
	protected void initListener(HdfsOperater hdfsOperater) {
		hdfsOperater.setHdfsTraversal(this);
	}
	
	public final void traversal(Path path) throws Exception {
		try{
			for (HdfsOperater hdfsOperater : listeners) {
				hdfsOperater.init(fileSystem);
			}
			if (!fileSystem.exists(path)) {
				throw new Exception("path [" + path.toString() + "] not exists on HDFS");
			}
			this.initRootPath(path);
			if (!isSkip(path)) {
				deepTraversal(fileSystem.makeQualified(path));
			} else {
				logger.info("skip the path [" + path.toString() + "]");
			}
		} finally {
			for (HdfsOperater hdfsOperater : listeners) {
				hdfsOperater.destory(fileSystem);
			}
		}
	}
	
	public final void traversal(Path[] paths) throws Exception {
		List<Path> pathList = new ArrayList<Path>();
		for (Path path : paths) {
			if (!fileSystem.exists(path)) {
				logger.warn("path [" + path.toString() + "] not exists, skipped");
				continue;
			}
			if (isSkip(path)) {
				logger.info("skip the path [" + path.toString() + "]");
				continue;
			}
			pathList.add(fileSystem.makeQualified(path));
		}
		initRootPath(pathList);
		for (HdfsOperater hdfsOperater : listeners) {
			hdfsOperater.init(fileSystem);
		}
		try{
			for (Path path : pathList) {
				deepTraversal(path);
			}
		} finally {
			for (HdfsOperater hdfsOperater : listeners) {
				hdfsOperater.destory(fileSystem);
			}
		}
	}
	
	public final void traversal(List<String> pathList) throws Exception {
		int size = pathList.size();
		Path[] paths = new Path[size];
		for (int i = 0; i < size; i++) {
			paths[i] = new Path(pathList.get(i));
		}
		traversal(paths);
	}
	
	private void initRootPath(List<Path> pathList) throws IOException {
		List<String> stringList = new ArrayList<String>();
		for (Path path : pathList) {
			if(fileSystem.isDirectory(path)) {
				stringList.add(path.toString() + IOUtils.DIR_SEPARATOR);
			}
		}
		initRootPath(Utils.samePath(stringList));
	}
	
	private void initRootPath(String path) {
		if (StringUtils.isBlank(path)) {
			this.traversalRootPath = fileSystem.makeQualified(new Path("/"));
		} else {
			initRootPath(new Path(path));
		}
	}
	
	private void initRootPath(Path path) {
		Path pathTemp = fileSystem.makeQualified(path);
		Path pathParentTemp = pathTemp.getParent();
		if (pathParentTemp!= null) {
			this.traversalRootPath = pathParentTemp;
		} else {
			this.traversalRootPath = pathTemp;
		}
	}
	private void deepTraversal(Path path) throws IOException {
		if (fileSystem.isFile(path)) {
			for (HdfsOperater hdfsOperater : listeners) {
				hdfsOperater.fileOperate(fileSystem, path);
			}
		} else if (fileSystem.isDirectory(path)) {
			for (HdfsOperater hdfsOperater : listeners) {
				hdfsOperater.beforeDir(fileSystem, path);
			}
			int count = 0;
			int fileCount = 0;
			int dirCount = 0;
			Path pathTemp = null;
			for (FileStatus fileStatus : fileSystem.listStatus(path)) {
				pathTemp = fileStatus.getPath();
				if (isSkip(pathTemp)) {
					logger.info("skip the path [" + pathTemp.toString() + "]");
					continue;
				}
				if (fileSystem.isDirectory(pathTemp)) {
					dirCount++;
					if (config.isRecursion()) {
						deepTraversal(pathTemp);
					}
				} else if (fileSystem.isFile(pathTemp)) {
					fileCount++;
					for (HdfsOperater hdfsOperater : listeners) {
						hdfsOperater.fileOperate(fileSystem, pathTemp);
					}
				} else {
					logger.warn("neither file nor directory, skip [" + pathTemp.toString() + "]");
				}
				count++;
			}
			for (HdfsOperater hdfsOperater : listeners) {
				hdfsOperater.afterDir(fileSystem, path, count, fileCount, dirCount);
			}
		}
	}
	
	public void traversal(String path) throws Exception {
		path = FilenameUtils.normalizeNoEndSeparator(path, true); 
		traversal(new Path(path));
	}
	
	public String getRelativePath(Path path) {
		String pathTemp = StringUtils.substringAfter(path.toString(), traversalRootPath.toString());
		if (StringUtils.isBlank(pathTemp)) {
			return "." + IOUtils.DIR_SEPARATOR;
		}
		if (pathTemp.startsWith("/")) {
			return "." + pathTemp;
		}
		return pathTemp;
	}
	
	private boolean isSkip(Path path) throws IOException {
		String[] temp = null;
		if (fileSystem.isFile(path)) {
			temp = config.getFileSkips();
			if (temp != null) {
				return Utils.isMatch(path.toString(), temp);
			}
		} else if (fileSystem.isDirectory(path)) {
			temp = config.getDirSkips();
			if (temp != null) {
				return Utils.isMatch(path.toString(), temp); 
			}
		}
		return false;
	}
	
	public FileSystem getFileSystem() {
		return fileSystem;
	}

	public void setFileSystem(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	public HdfsTraversalConfig getConfig() {
		return config;
	}

	public void setConfig(HdfsTraversalConfig config) {
		this.config = config;
	}

	public Path getTraversalRootPath() {
		return traversalRootPath;
	}

	public void setTraversalRootPath(Path traversalRootPath) {
		this.traversalRootPath = traversalRootPath;
	}
	
}
