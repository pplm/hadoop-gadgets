package org.pplm.hadoop.gadgets.mrjob.test;

import java.io.IOException;

import org.junit.Test;
import org.pplm.hadoop.gadgets.mrjob.demo.RepeatMr;

public class RepeatDemoTest {
	
	@Test
	public void RepeatTest() throws ClassNotFoundException, IOException, InterruptedException {
		RepeatMr.statup("/user/piao/sort/ts_sort.csv", "/tmp/sort");
	}
}
