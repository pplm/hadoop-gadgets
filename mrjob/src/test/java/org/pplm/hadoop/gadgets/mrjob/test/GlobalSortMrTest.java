package org.pplm.hadoop.gadgets.mrjob.test;

import java.io.IOException;

import org.junit.Test;
import org.pplm.hadoop.gadgets.mrjob.demo.GlobalSortMr;

public class GlobalSortMrTest {
	
	@Test
	public void globalSortMrTest () throws ClassNotFoundException, IOException, InterruptedException {
		GlobalSortMr.statup("/tmp/sort", "/tmp/sorted");
	}
	
}
