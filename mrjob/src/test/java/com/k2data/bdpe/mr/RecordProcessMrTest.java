package com.k2data.bdpe.mr;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import org.pplm.hadoop.gadgets.schema.key.KeyParseException;
import org.pplm.hadoop.gadgets.schema.key.KeysManager;

public class RecordProcessMrTest {
	
	public static void main(String[] args) throws KeyParseException, IOException {
		String file = Thread.currentThread().getContextClassLoader().getResource("key_test.csv").getPath();
		KeysManager keysManager = new KeysManager();
		keysManager.initKey(FileUtils.readLines(new File(file), "utf-8"));
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJar("f:/mrjob.jar");
		job.setJobName("Job name:" + "RecordProcessMrTest");
		
	}
}
