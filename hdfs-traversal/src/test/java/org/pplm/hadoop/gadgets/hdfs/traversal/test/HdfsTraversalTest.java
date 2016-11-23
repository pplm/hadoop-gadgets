package org.pplm.hadoop.gadgets.hdfs.traversal.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsTraversal;
import org.pplm.hadoop.gadgets.hdfs.traversal.utils.Utils;

public class HdfsTraversalTest {
	
	public void relativePathTest() throws Exception {
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.newInstance(new Configuration());
			HdfsTraversal hdfsTraversal = new HdfsTraversal(fileSystem);
			hdfsTraversal.traversal("/user/fit/gaojz/");
		} finally {
			if (fileSystem != null) {
				fileSystem.close();
			}
		}
	}
	
	public void sameTest() {
		List<String> stringList = new ArrayList<String>();
		stringList.add("/m2m/data05/qcd/now/TP_GPS_RESULT_201502");
		assertEquals(Utils.samePath(stringList), "/m2m/data05/qcd/now/");
		stringList.add("/m2m/data05/qcd/now/TP_GPS_RESULT_201502");
		assertEquals(Utils.samePath(stringList), "/m2m/data05/qcd/now/");
		stringList.add("/m2m/data05/gpsresult/GPSRESULT_PART_201011");
		stringList.add("/m2m/data05/lvd/");
		stringList.add("/m2m/data05/qcd/exception");
		stringList.add("/m2m/data05/qcx/result/SANY_QZJ_RESULT_P201007");
		assertEquals(Utils.samePath(stringList), "/m2m/data05/");
		stringList.add("/m2m/data06/equipposhistrec");
		assertEquals(Utils.samePath(stringList), "/m2m/");
	}
	
	@Test
	public void pathsTest() throws Exception {
		List<String> stringList = new ArrayList<String>();
		stringList.add("/m2m/data05/qcd/now/TP_GPS_RESULT_201502");
		stringList.add("/m2m/data05/qcd/now/TP_GPS_RESULT_201502");
		stringList.add("/m2m/data05/gpsresult/GPSRESULT_PART_201011");
		stringList.add("/m2m/data05/lvd/");
		stringList.add("/m2m/data05/qcd/exception");
		stringList.add("/m2m/data05/qcx/result/SANY_QZJ_RESULT_P201007");
		stringList.add("/m2m/data06/equipposhistrec");
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.newInstance(new Configuration());
			HdfsTraversal hdfsTraversal = new HdfsTraversal(fileSystem);
			hdfsTraversal.traversal(stringList);
			System.out.println("***" + hdfsTraversal.getTraversalRootPath());
		} finally {
			if (fileSystem != null) {
				fileSystem.close();
			}
		}
	}
}
