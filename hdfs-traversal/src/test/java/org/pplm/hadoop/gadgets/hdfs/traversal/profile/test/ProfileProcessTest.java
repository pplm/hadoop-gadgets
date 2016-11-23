package org.pplm.hadoop.gadgets.hdfs.traversal.profile.test;

import org.junit.Test;
import org.pplm.hadoop.gadgets.hdfs.traversal.profile.ProfileProcess;

public class ProfileProcessTest {
	@Test
	public void main() throws Exception {
		ProfileProcess.Process("/k2data", "f:/tmp/hp_output");
	}
}
