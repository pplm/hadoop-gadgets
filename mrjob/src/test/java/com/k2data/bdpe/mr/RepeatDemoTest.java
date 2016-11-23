package com.k2data.bdpe.mr;

import java.io.IOException;

import org.junit.Test;

import com.k2data.bdpe.mr.demo.RepeatMr;

public class RepeatDemoTest {
	
	@Test
	public void RepeatTest() throws ClassNotFoundException, IOException, InterruptedException {
		RepeatMr.statup("/user/piao/sort/ts_sort.csv", "/tmp/sort");
	}
}
