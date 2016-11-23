package org.pplm.hadoop.gadgets.hdfs.extraction;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

public class IoUtils {
	
	public static int copyLines(InputStream inputStream, OutputStream outputStream, String charset, int lineCount) throws IOException {
		int count = 0;
		LineIterator lineIterator = IOUtils.lineIterator(inputStream, charset);
		CharSequence line = null;
		while(lineIterator.hasNext()) {
			line = lineIterator.next();
			IOUtils.write(line, outputStream, charset);
			IOUtils.write(IOUtils.LINE_SEPARATOR, outputStream, charset);
			if (++count >= lineCount) {
				break;
			}
		}
		return count;
	}
	
}
