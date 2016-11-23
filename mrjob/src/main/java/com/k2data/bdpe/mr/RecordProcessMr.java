package com.k2data.bdpe.mr;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Utils.OutputFileUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.pplm.hadoop.gadgets.schema.key.KeyParseException;
import org.pplm.hadoop.gadgets.schema.key.KeysManager;
import org.pplm.hadoop.gadgets.schema.key.ValueKey;
import org.pplm.hadoop.gadgets.schema.record.RecordParseException;
import org.pplm.hadoop.gadgets.schema.record.RecordParser;
import org.pplm.hadoop.gadgets.schema.record.TaggedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordProcessMr {
	
	public static final String KEY_SEPARATOR_ITEM = "bdpe.record.mr.separator.item";
	public static final String DEFAULT_SEPARATOR_ITEM = ",";
	
	public static final String KEY_SEPARATOR_KV = "bdpe.record.mr.separator.kv";
	public static final String DEFAULT_SEPARATOR_KV = ":";
	
	public static final String KEY_STATISTIC_ONLY = "bdpe.record.mr.statistics.only";
	public static final boolean DEFAULT_STATISTIC_ONLY = true;
	
	public enum Counter {
		TOTAL_LINES("bdpe.record.mr.counter.lines.total"),
		SPACE_LINES("bdpe.record.mr.counter.lines.space"),
		INVALID_LINES("bdpe.record.mr.counter.lines.invalid"),
		TAGGED_LINES("bdpe.record.mr.counter.lines.tagged"),
		PAIRS("bdpe.record.mr.counter.pairs"),
		MISMATCHS("bdpe.record.mr.counter.key.mismatch"),
		VALIDS("bdpe.record.mr.counter.value.valid"),
		INVALIDS("bdpe.record.mr.counter.value.invalid"),
		INVALID_DATA_TYPES("bdpe.record.mr.counter.value.invalid.data.type"),
		INVALID_LT_MINS("bdpe.record.mr.counter.value.lt.min"),
		INVALID_GT_MAXS("bdpe.record.mr.counter.value.gt.max"),
		INVALID_PRECISIONS("bdpe.record.mr.counter.value.invalid.precision"),
		INVALID_SIGNS("bdpe.record.mr.counter.value.invalid.sign"),
		NULL_VALUES("bdpe.record.mr.counter.value.null");
		
		public String value;
		
		Counter(String value) {
			this.value = value;
		}
	}
	
	public static class RecordProcessMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		private KeysManager keysManager;
		private RecordParser recordParser;
		private boolean statisticOnly;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			recordParser = new RecordParser(config.get(KEY_SEPARATOR_ITEM, DEFAULT_SEPARATOR_ITEM), config.get(KEY_SEPARATOR_KV, DEFAULT_SEPARATOR_KV));
			statisticOnly = config.getBoolean(KEY_STATISTIC_ONLY, DEFAULT_STATISTIC_ONLY);
			URI[] uris = context.getCacheFiles();
			if (uris.length < 1) {
				throw new IOException("key file not exists");
			}
			FileSystem fileSystem = FileSystem.get(config);
			InputStream inputStream = null;
			keysManager = new KeysManager();
			try {
				inputStream = fileSystem.open(new Path(uris[0]));
				keysManager.initKey(IOUtils.readLines(inputStream));
			} catch (KeyParseException e) {
				throw new IOException(e);
			} finally {
				if (inputStream != null) {
					inputStream.close();
				}
			}
		}
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.getCounter(Counter.TOTAL_LINES).increment(1);
			String line = value.toString();
			if (StringUtils.isBlank(line)) {
				context.getCounter(Counter.SPACE_LINES).increment(1);
				return;
			}
			try {
				Map<String, String> map = recordParser.parse(line);
				context.getCounter(Counter.PAIRS).increment(map.size());
				int flag = 0;
				boolean invalid = false;
				boolean tagged = false;
				TaggedRecord taggedRecord = new TaggedRecord(line);
				for (Entry<String, String> entry : map.entrySet()) {
					invalid = false;
					flag = keysManager.validate(entry.getKey(), entry.getValue());
					if (flag == 0 || flag == KeysManager.VALUE_NULL) {
						context.getCounter(Counter.VALIDS).increment(1);
					}
					if (flag != 0) {
						if (flag == KeysManager.VALUE_NULL) {
							context.getCounter(Counter.NULL_VALUES).increment(1);
						} else {
							taggedRecord.addInvalid(entry.getKey(), entry.getValue(), flag);
							if (flag == KeysManager.KEY_MISMATCH) {
								context.getCounter(Counter.MISMATCHS).increment(1);
								tagged = true;
							} else {
								if ((flag & ValueKey.INVALD_DATA_TYPE) != 0) {
									context.getCounter(Counter.INVALID_DATA_TYPES).increment(1);
									invalid = true;
								}
								if ((flag & ValueKey.INVALD_GREAT_THAN_MAX) != 0) {
									context.getCounter(Counter.INVALID_GT_MAXS).increment(1);
									invalid = true;
								}
								if ((flag & ValueKey.INVALD_LESS_THAN_MIN) != 0) {
									context.getCounter(Counter.INVALID_LT_MINS).increment(1);
									invalid = true;
								}
								if ((flag & ValueKey.INVALD_PRECISION) != 0) {
									context.getCounter(Counter.INVALID_PRECISIONS).increment(1);
									invalid = true;
								}
								if ((flag & ValueKey.INVALD_SIGNED) != 0) {
									context.getCounter(Counter.INVALID_SIGNS).increment(1);
									invalid = true;
								}
								if (invalid) {
									context.getCounter(Counter.INVALIDS).increment(1);
									tagged = true;
								}
							}
						}
					}
				}
				if (tagged) {
					context.getCounter(Counter.TAGGED_LINES).increment(1);
				}
				if (!statisticOnly && tagged) {
					context.write(key, new Text(taggedRecord.toString()));
				}
			} catch (RecordParseException e) {
				context.getCounter(Counter.INVALID_LINES).increment(1);
				if (!statisticOnly) {
					context.write(key, new Text(line + "\t" + "invalid"));
				}
			}
		}
	}
	
	public static class GlobalSortPartitioner extends Partitioner<LongWritable, Text> implements Configurable {

		private Logger logger = LoggerFactory.getLogger(GlobalSortPartitioner.class);
		
		private long length;
		
		private Configuration config;
		
		@Override
		public int getPartition(LongWritable key, Text value, int numPartitions) {
			if (length == 0) {
				throw new RuntimeException("length 0");
			}
			return ((int)(key.get() / (length / numPartitions + 1))) % numPartitions;
		}

		@Override
		public void setConf(Configuration conf) {
			config = conf;
			try {
				FileSystem fileSystem = FileSystem.get(conf);
				String pathsStr = conf.get(FileInputFormat.INPUT_DIR);
				String[] paths = StringUtils.split(pathsStr, org.apache.hadoop.util.StringUtils.COMMA);
				this.length = pathsLength(fileSystem, paths);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}

		@Override
		public Configuration getConf() {
			return config;
		}
		
		private long pathsLength(FileSystem fileSystem, String[] paths) throws IOException {
			long result = 0;
			Path path = null;
			for (String pathStr : paths) {
				path = new Path(pathStr);
				if (fileSystem.isFile(path)) {
					result += fileSystem.getFileStatus(path).getLen();
				}
				FileStatus[] fileStatuses = fileSystem.listStatus(path, new OutputFileUtils.OutputFilesFilter());
				for (FileStatus fileStatus : fileStatuses) {
					if (fileStatus.isFile()) {
						result += fileStatus.getLen();
					}
				}
			}
			return result;
		}
	}
	
	public static class RecordProcessReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text value: values) {
			      context.write(key, value);
			}
		}
	}
	
}
