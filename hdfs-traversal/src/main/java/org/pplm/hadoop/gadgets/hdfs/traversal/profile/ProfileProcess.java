package org.pplm.hadoop.gadgets.hdfs.traversal.profile;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsOperater;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsTraversal;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.HadoopConfig;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Utils;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.ReportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author OracleGao
 *
 */
public class ProfileProcess extends HdfsOperater {
	
	private Logger logger = LoggerFactory.getLogger(ProfileProcess.class);
	
	private Map<Path, ProfileBean> hdfsProfileBeanMap = new LinkedHashMap<Path, ProfileBean>();
	
	private DateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public ProfileProcess(Configuration config) {
		super("profile", config);
	}

	@Override
	public void init(FileSystem fileSystem) throws Exception {
		super.initReporter(fileSystem, ProfileBean.REPORT_FILE_NAME, ProfileBean.class);
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
		super.initConsume(path.toString());
		try {
			ProfileBean hdfsProfileBean = new ProfileBean();
			FileStatus fileStatus = fileSystem.getFileStatus(path);
			hdfsProfileBean.setName(path.getName());
			Path parentPath = path.getParent();
			if (parentPath != null) {
				hdfsProfileBean.setPath(parentPath.toUri().getRawPath());
			} else {
				hdfsProfileBean.setPath("/");
			}
			hdfsProfileBean.setOwner(fileStatus.getOwner());
			hdfsProfileBean.setGroup(fileStatus.getGroup());
			hdfsProfileBean.setUri(fileStatus.getPath().toString());
			hdfsProfileBean.setAuth(fileStatus.getPermission().toString());
			hdfsProfileBean.setDirectory(true);
			hdfsProfileBeanMap.put(path, hdfsProfileBean);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public void afterDir(FileSystem fileSystem, Path path, int count, int fileCount, int dirCount) {
		ProfileBean hdfsProfileBean = hdfsProfileBeanMap.get(path);
		if (hdfsProfileBean != null) {
			hdfsProfileBean.setCount(count);
			hdfsProfileBean.setDirCount(dirCount);
			hdfsProfileBean.setFileCount(fileCount);
			ProfileBean hdfsProfileBeanParent = hdfsProfileBeanMap.get(path.getParent());
			long length = hdfsProfileBean.getLength();
			if (hdfsProfileBeanParent != null) {
				hdfsProfileBeanParent.setLength(hdfsProfileBeanParent.getLength() + length);
			}
			hdfsProfileBean.setReadableLength(Utils.ReadableFileLength(length));
		}
		try {
			reporter.reportRecord(hdfsProfileBean);
		} catch (ReportException e) {
			logger.error(e.getMessage(), e);
		}
		logger.info("directory [" + path.toString() + " ] consume time [" + super.getConsume(path.toString()) + "] ms");
	}

	@Override
	public void fileOperate(FileSystem fileSystem, Path path) {
		try {
			ProfileBean hdfsProfileBean = new ProfileBean();
			hdfsProfileBean.setDirectory(false);
			FileStatus fileStatus = fileSystem.getFileStatus(path);
			hdfsProfileBean.setName(path.getName());
			hdfsProfileBean.setOwner(fileStatus.getOwner());
			hdfsProfileBean.setGroup(fileStatus.getGroup());
			hdfsProfileBean.setUri(fileStatus.getPath().toString());
			hdfsProfileBean.setAuth(fileStatus.getPermission().toString());
			hdfsProfileBean.setBlockSize(fileStatus.getBlockSize());
			hdfsProfileBean.setEncrypted(fileStatus.isEncrypted());
			hdfsProfileBean.setReplications(fileStatus.getReplication());
			long temp = fileStatus.getLen();
			hdfsProfileBean.setLength(temp);
			hdfsProfileBean.setReadableLength(Utils.ReadableFileLength(temp));
			temp = fileStatus.getAccessTime();
			hdfsProfileBean.setAccessTime(temp);
			if (temp > 0) {
				hdfsProfileBean.setAccessTimeReadable(dataFormat.format(new Date(temp)));
			} else {
				hdfsProfileBean.setAccessTimeReadable("-");
			}
			temp = fileStatus.getModificationTime();
			hdfsProfileBean.setModifiedTime(temp);
			if (temp > 0) {
				hdfsProfileBean.setModifiedTimeReadable(dataFormat.format(new Date(temp)));
			} else {
				hdfsProfileBean.setModifiedTimeReadable("-");
			}
			Path parentPath = path.getParent();
			hdfsProfileBean.setPath(parentPath.toUri().getRawPath());
			ProfileBean hdfsProfileBeanParent = hdfsProfileBeanMap.get(parentPath);
			if (hdfsProfileBeanParent != null) {
				hdfsProfileBeanParent.setLength(hdfsProfileBeanParent.getLength() + hdfsProfileBean.getLength());
			}
			reporter.reportRecord(hdfsProfileBean);
		} catch (IOException | ReportException e) {
			logger.error(e.getMessage(), e);
		}
	}

	public static void Process(String hdfsInputPath, String localOutputPath) throws Exception {
		FileSystem fileSystem = null;
		try {
			Configuration config = new Configuration();
			if (StringUtils.isNotBlank(localOutputPath)) {
				config.set(HadoopConfig.KEY_LOCAL_OUTPUT_PATH, localOutputPath);
			}
			fileSystem = FileSystem.newInstance(config);
			HdfsOperater hdfsOperater = new ProfileProcess(config);
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
		ProfileProcess.Process(hdfsInputPath, localOutputPath);
	}

}
