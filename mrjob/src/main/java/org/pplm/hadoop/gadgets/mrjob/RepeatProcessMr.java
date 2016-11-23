package org.pplm.hadoop.gadgets.mrjob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Utils.OutputFileUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepeatProcessMr {
	
	public static String KEY_SHA_ENABLE = "bdpe.mr.repeat.sha.enable";
	public static boolean DEFAULT_SHA_ENABLE = true;
	
	public static String KEY_STATISTICS_ONLY = "bdpe.mr.repeat.statistics.only";
	public static boolean DEFAULT_STATISTICS_ONLY = true;
	
	public enum Counter {
		TOTAL("bdpe.mr.repeat.counter.total"),
		REPEAT_TOTAL("bdpe.mr.repeat.counter.repeat.total"),
		REPEAT("bdpe.mr.repeat.counter.repeat"),
		SPACE("bdpe.mr.repeat.counter.space");
		
		public String value;
		
		Counter(String value) {
			this.value = value;
		}
	}
	
	public static class RepeatProcessMapper extends Mapper<LongWritable, Text, BytesWritable, RecordWritable> {
		private static Logger logger = LoggerFactory.getLogger(RepeatProcessMapper.class);
		
		private MessageDigest messageDigest;
		private boolean shaEnable;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			shaEnable = context.getConfiguration().getBoolean(KEY_SHA_ENABLE, DEFAULT_SHA_ENABLE);
			if (shaEnable) {
				try {
					messageDigest = MessageDigest.getInstance("SHA-1");
				} catch (NoSuchAlgorithmException e) {
					shaEnable = false;
					logger.error(e.getMessage(), e);
				}
			}
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String record = value.toString().trim();
			if (org.apache.commons.lang.StringUtils.isBlank(record)) {
				context.getCounter(Counter.SPACE).increment(1);
				return;
			}
			BytesWritable bytesWritable = null;
			byte[] bytes = record.getBytes();
			if (shaEnable) {
				bytesWritable = new BytesWritable(messageDigest.digest(bytes));
			} else {
				bytesWritable = new BytesWritable(bytes);
			}
			context.write(bytesWritable, new RecordWritable(key, value));
		}
	}
	
	public static class RecordWritable implements WritableComparable<RecordWritable> {
		
		private LongWritable offset;
		private Text record;
		
		public RecordWritable() {
			super();
		}
		
		public RecordWritable(LongWritable offset, Text record) {
			super();
			this.offset = offset;
			this.record = record;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			offset.write(out);
			record.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			offset = new LongWritable(in.readLong());
			record = new Text();
			record.readFields(in);
		}	

		@Override
		public int compareTo(RecordWritable recordWritable) {
			return 0;
		}
		
		public LongWritable getOffset() {
			return offset;
		}

		public void setOffset(LongWritable offset) {
			this.offset = offset;
		}

		public Text getRecord() {
			return record;
		}

		public void setRecord(Text record) {
			this.record = record;
		}

	}
	
	public static class RepeatProcessReducer extends Reducer<BytesWritable, RecordWritable, LongWritable, Text> {
		private boolean statisticsOnly;
		
		public void setup(Context context) throws IOException, InterruptedException {
			 statisticsOnly = context.getConfiguration().getBoolean(KEY_STATISTICS_ONLY, DEFAULT_STATISTICS_ONLY);
		}
		
		@Override
		public void reduce(BytesWritable key, Iterable<RecordWritable> values, Context context)  throws IOException, InterruptedException {
			long minOffset = -1;
			long temp = 0;
			RecordWritable value = null;
			long count = 0;
			for (RecordWritable recordWritable : values) {
				temp = recordWritable.offset.get();
				count++;
				if (minOffset >= 0) {
					if (minOffset > temp) {
						minOffset = temp;
					}
				} else {
					minOffset = temp;
					value = recordWritable;
				}
			}
			context.getCounter(Counter.TOTAL).increment(count);
			if (count > 1) {
				context.getCounter(Counter.REPEAT_TOTAL).increment(count);
				context.getCounter(Counter.REPEAT).increment(1);
			}
			if (!statisticsOnly) {
				context.write(new LongWritable(minOffset), value.record);
			}
		}
	}
	
	public static class GlobalSortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		@Override
		public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] pair = value.toString().split(context.getConfiguration().get(TextOutputFormat.SEPERATOR, "\t"));
			context.write(new LongWritable(Long.parseLong(pair[0])), new Text(pair[1]));
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
				String[] paths = StringUtils.split(pathsStr, StringUtils.COMMA);
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
	
	public static class GlobalSortReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, NullWritable.get());
			}
		}
	}
	
}
