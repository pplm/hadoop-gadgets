package com.k2data.bdpe.mr;

import java.io.IOException;

import org.junit.Test;

import com.k2data.bdpe.mr.demo.GlobalSortMr;

public class GlobalSortMrTest {
	
	@Test
	public void globalSortMrTest () throws ClassNotFoundException, IOException, InterruptedException {
		GlobalSortMr.statup("/tmp/sort", "/tmp/sorted");
	}
	
}
