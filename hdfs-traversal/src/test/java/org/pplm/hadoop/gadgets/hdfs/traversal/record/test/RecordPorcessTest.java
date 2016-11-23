package org.pplm.hadoop.gadgets.hdfs.traversal.record.test;

import org.junit.Test;
import org.pplm.hadoop.gadgets.hdfs.traversal.record.RecordProcess;

public class RecordPorcessTest {
	@Test
	public void recordPorcessTest() throws Exception {
		RecordProcess.Process("/user/piao/record", "f:/tmp/re_output");
	}
}
