package org.pplm.hadoop.gadgets.mrjob;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.pplm.hadoop.gadgets.schema.Schema;
import org.pplm.hadoop.gadgets.schema.SchemaManager;
import org.pplm.hadoop.gadgets.schema.SchemaParseException;
import org.pplm.hadoop.gadgets.schema.SchemaParser;
import org.pplm.hadoop.gadgets.schema.TextSchemaParser;

public class SchemaProcessMr {
	
	public static final String KEY_SEPARATOR_ITEM = "bdpe.mr.schema.separator.item";
	public static final String DEFAULT_SEPARATOR_ITEM = ",";
	
	public static final String KEY_SEPARATOR_KV = "bdpe.mr.schema.separator.kv";
	public static final String DEFAULT_SEPARATOR_KV = ":";
	
	public static final String KEY_FLAG_ALL = "bdpe.mr.schema.flag.all";
	public static final boolean DEFAULT_FLAG_ALL = false;
	
	public enum Counter {
		LINE_TOTAL("bdpe.mr.schema.counter.line.total"), 
		LINE_SPACE("bdpe.mr.schema.counter.line.space"), 
		LINE_INVALID("bdpe.mr.schema.counter.line.invalid"),
		SCHEMA("bdpe.mr.schema.counter.schema"), 
		SCHEMA_MOST("bdpe.mr.schema.counter.schema.most");
		
		public String value;
		
		Counter(String value) {
			this.value = value;
		}
	}
	
	public static class SchemaProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		private SchemaParser schemaParser;
		private String itemSeparator;
		private String kvSeparator;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			itemSeparator = config.get(KEY_SEPARATOR_ITEM, DEFAULT_SEPARATOR_ITEM);
			kvSeparator = config.get(KEY_SEPARATOR_KV, DEFAULT_SEPARATOR_KV);
			System.out.println("item: " + itemSeparator + ", kv: " + kvSeparator);
			
			schemaParser = new TextSchemaParser(itemSeparator, kvSeparator);
		}
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if (StringUtils.isBlank(line)) {
				context.getCounter(Counter.LINE_SPACE).increment(1);
				return;
			}
			Schema schema = null;
			try {
				schema = schemaParser.parse(line);
				if (schema == null) {
					System.out.println("1231322");
				}
			} catch (SchemaParseException e) {
				context.getCounter(Counter.LINE_INVALID).increment(1);
				return;
			}
			context.write(new Text(schema.toString(itemSeparator)), NullWritable.get());
		}
	}

	public static class SchemaProcessReducer extends Reducer<Text, NullWritable, Text, Text> {
		private SchemaParser schemaParser;
		private String itemSeparator;
		private String kvSeparator;
		private SchemaManager schemaManager;
		private boolean allFlag;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			itemSeparator = config.get(KEY_SEPARATOR_ITEM, DEFAULT_SEPARATOR_ITEM);
			kvSeparator = config.get(KEY_SEPARATOR_KV, DEFAULT_SEPARATOR_KV);
			schemaParser = new TextSchemaParser(itemSeparator, kvSeparator);
			allFlag = config.getBoolean(KEY_FLAG_ALL, DEFAULT_FLAG_ALL);
			schemaManager = new SchemaManager(allFlag);
		}
		
		@Override
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			Schema schema = schemaParser.parseKeys(key.toString());
			long count = 0;
			for (@SuppressWarnings("unused")NullWritable value : values) {
				count++;
			}
			schemaManager.putSchema(schema, count);
			context.getCounter(Counter.LINE_TOTAL).increment(count);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.getCounter(Counter.SCHEMA).increment(schemaManager.getSchemaCount());
			context.getCounter(Counter.SCHEMA_MOST).increment(schemaManager.getMostSchemaCount());
			if (schemaManager.getSchemaCount() > 0) {
				Text space = new Text("");
				context.write(new Text(schemaManager.getAllKeySchema().toString(itemSeparator)), space);
				context.write(new Text(schemaManager.getMaxSchema().toString(itemSeparator)), space);
				context.write(new Text(schemaManager.getMinSchema().toString(itemSeparator)), space);
				context.write(new Text(schemaManager.getMostSchema().toString(itemSeparator)), new Text(String.valueOf(schemaManager.getMostSchemaCount())));
				context.write(new Text(String.valueOf(schemaManager.getSchemaCount())), space);
				if (allFlag) {
					for (Entry<Schema, Long> entry : schemaManager.getSchemaCountSet()) {
						context.write(new Text(entry.getKey().toString(itemSeparator)), new Text(String.valueOf(entry.getValue())));
					}
				}
			}
		}
	}
	
}
