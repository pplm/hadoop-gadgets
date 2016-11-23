package org.pplm.hadoop.gadgets.hdfs.traversal.schema.test;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.pplm.hadoop.gadgets.hdfs.traversal.HdfsTraversalConfig;
import org.pplm.hadoop.gadgets.hdfs.traversal.schema.SchemaProcess;

public class SchemaProcessTest {
	@Test
	public void mainTest() throws Exception {
		SchemaProcess.Process("/user/piao/schema", "f:/tmp/sp_output");
	}
}
