package org.pplm.hadoop.gadgets.hdfs.traversal.profile;

import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.IReportable;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.reporter.ReportHeaderItem;

/**
 * HDFS文件系统画像Bean
 * 目录文件和普通文件复用该Bean
 * @author OracleGao
 */
public class ProfileBean implements IReportable {
	
	public static String REPORT_FILE_NAME = "profileReport.csv"; 
	
	/**
	 * 目录：true
	 * 文件：false
	 */
	@ReportHeaderItem(name = "directory")
	private boolean directory;
	/**
	 * 权限
	 */
	@ReportHeaderItem(name = "auth")
	private String auth;
	
	/**
	 * 文件名
	 */
	@ReportHeaderItem(name = "name")
	private String name;
	
	@ReportHeaderItem(name = "path")
	private String path;
	
	/**
	 * DFS中的完整uri
	 */
	@ReportHeaderItem(name = "uri")
	private String uri;
	/**
	 * 目录：目录大小（目录下的所有文件大小的汇总）
	 * 文件：文件大小
	 */
	@ReportHeaderItem(name = "length")
	private long length;
	
	@ReportHeaderItem(name = "readable length")
	private String readableLength;
	
	/**
	 * 目录：0
	 * 文件：最后访问时间
	 */
	private long accessTime;
	
	@ReportHeaderItem(name = "access time")
	private String accessTimeReadable;
	
	/**
	 * 修改时间
	 */
	private long modifiedTime;

	@ReportHeaderItem(name = "modified time")
	private String modifiedTimeReadable;
	
	/**
	 * 文件Owner
	 */
	@ReportHeaderItem(name = "owner")
	private String owner;

	/**
	 * 文件所属组
	 */
	@ReportHeaderItem(name = "group")
	private String group;
	
	/**
	 * 目录：目录下文件个数（包括目录）
	 * 文件：0
	 */
	@ReportHeaderItem(name = "count")
	private int count;
	
	@ReportHeaderItem(name = "file count")
	private int fileCount;
	
	@ReportHeaderItem(name = "directory count")
	private int dirCount;
	
	/**
	 * 目录：0
	 * 文件：副本数
	 */
	@ReportHeaderItem(name = "replications")
	private int replications;
	
	/**
	 * 目录：0
	 * 文件：块大小
	 */
	@ReportHeaderItem(name = "block size")
	private long blockSize;
	
	/**
	 * 目录：false
	 * 文件：是否加密，true：加密，false：非加密 
	 */
	@ReportHeaderItem(name = "encrypted")
	private boolean encrypted;

	public ProfileBean() {
		super();
	}

	public boolean isDirectory() {
		return directory;
	}

	public void setDirectory(boolean directory) {
		this.directory = directory;
	}

	public String getAuth() {
		return auth;
	}

	public void setAuth(String auth) {
		this.auth = auth;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public String getReadableLength() {
		return readableLength;
	}

	public void setReadableLength(String readableLength) {
		this.readableLength = readableLength;
	}

	public long getAccessTime() {
		return accessTime;
	}

	public void setAccessTime(long accessTime) {
		this.accessTime = accessTime;
	}

	public long getModifiedTime() {
		return modifiedTime;
	}

	public void setModifiedTime(long modifiedTime) {
		this.modifiedTime = modifiedTime;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getFileCount() {
		return fileCount;
	}

	public void setFileCount(int fileCount) {
		this.fileCount = fileCount;
	}

	public int getDirCount() {
		return dirCount;
	}

	public void setDirCount(int dirCount) {
		this.dirCount = dirCount;
	}

	public int getReplications() {
		return replications;
	}

	public void setReplications(int replications) {
		this.replications = replications;
	}

	public long getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(long blockSize) {
		this.blockSize = blockSize;
	}

	public boolean isEncrypted() {
		return encrypted;
	}

	public void setEncrypted(boolean encrypted) {
		this.encrypted = encrypted;
	}

	public String getAccessTimeReadable() {
		return accessTimeReadable;
	}

	public void setAccessTimeReadable(String accessTimeReadable) {
		this.accessTimeReadable = accessTimeReadable;
	}

	public String getModifiedTimeReadable() {
		return modifiedTimeReadable;
	}

	public void setModifiedTimeReadable(String modifiedTimeReadable) {
		this.modifiedTimeReadable = modifiedTimeReadable;
	}

}
