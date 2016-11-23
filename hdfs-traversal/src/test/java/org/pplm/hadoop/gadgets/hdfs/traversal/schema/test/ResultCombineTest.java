package org.pplm.hadoop.gadgets.hdfs.traversal.schema.test;

import org.junit.Test;
import org.pplm.hadoop.gadgets.hdfs.traversal.schema.SchemaProcess;

public class ResultCombineTest {
	@Test
	public void mainTest() throws Exception {
		SchemaProcess.Process("/user/fit/gaojz/input/schema", "f:/tmp/output");
	}
}
