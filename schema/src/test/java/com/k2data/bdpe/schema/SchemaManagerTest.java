package com.k2data.bdpe.schema;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.pplm.hadoop.gadgets.schema.SchemaManager;

public class SchemaManagerTest {
	
	
	public void parseTest() throws Exception {
		boolean flag = true;
		FileSystem fileSystem = FileSystem.get(new Configuration());
		InputStream inputStream = fileSystem.open(new Path("/tmp/hdfsOperater/tmp/schemaProcess/1449740218461/schema.txt/part-r-00001"));
		SchemaManager schemaManager0 = SchemaManager.Parse(inputStream, "utf-8", flag, "#", "\t");
		inputStream.close();
		inputStream = fileSystem.open(new Path("/tmp/hdfsOperater/tmp/schemaProcess/1449740218461/schema.txt/part-r-00000"));
		SchemaManager schemaManager1 = SchemaManager.Parse(inputStream, "utf-8", flag, "#", "\t");
		SchemaManager schemaManager = new SchemaManager(flag);
		schemaManager.combine(schemaManager0);
		schemaManager.combine(schemaManager1);
		OutputStream outputStream = new FileOutputStream("f:/tmp/test.txt");
		schemaManager.mrSerialization(outputStream, "utf-8", "#", "\t");
		outputStream.flush();
		outputStream.close();
		System.out.println();
	}
	@Test
	public void parse1Test()  throws Exception {
		boolean flag = true;
		FileSystem fileSystem = FileSystem.get(new Configuration());
		InputStream inputStream = fileSystem.open(new Path("/tmp/hdfsOperater/tmp/schemaProcess/1449747449469/test/empty"));
		SchemaManager schemaManager = SchemaManager.Parse(inputStream, "utf-8", flag, "#", "\t");
		schemaManager.getAllKeySchema();
		inputStream.close();
	}
	
}
