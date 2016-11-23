package org.pplm.hadoop.gadgets.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

/**
 * 
 * @author OracleGao
 *
 */
public class SchemaManager {
	private Schema allKeySchema = new Schema();
	private Schema maxSchema = new Schema();
	private Schema minSchema = null;
	
	private Schema mostSchema = new Schema();
	private long mostSchemaCount;
	
	private long schemaCount;
	
	private Map<Schema, Long> schemaCountMap = new HashMap<Schema, Long>();
	private boolean allSchemaCountFlag;

	public SchemaManager() {
		super();
	}

	public SchemaManager(boolean allSchemaCountFlag) {
		super();
		this.allSchemaCountFlag = allSchemaCountFlag;
	}

	public void putSchema(Schema schema, long count) {
		if (schema.size() > maxSchema.size()) {
			maxSchema = schema;
		}
		if (minSchema == null || schema.size() < minSchema.size()) {
			minSchema = schema;
		}
		allKeySchema.combine(schema);
		if (count > mostSchemaCount) {
			mostSchema = schema;
			mostSchemaCount = count;
		}
		schemaCount += count;
		if (allSchemaCountFlag) {
			schemaCountMap.put(schema, count);
		}
	}

	public void combine(SchemaManager schemaManager) {
		this.allKeySchema.combine(schemaManager.allKeySchema);
		if (this.maxSchema.size() < schemaManager.maxSchema.size()) {
			this.maxSchema = schemaManager.maxSchema;
		}
		if (this.minSchema == null || this.minSchema.size() > schemaManager.minSchema.size()) {
			this.minSchema = schemaManager.minSchema;
		}
		if (this.mostSchemaCount < schemaManager.mostSchemaCount) {
			this.mostSchemaCount = schemaManager.mostSchemaCount;
			this.mostSchema = schemaManager.mostSchema;
		}
		this.schemaCount += schemaManager.schemaCount;
		if (this.allSchemaCountFlag) {
			this.schemaCountMap.putAll(schemaManager.schemaCountMap);
		}
	}
	
	public Set<Entry<Schema, Long>> getSchemaCountSet() {
		return schemaCountMap.entrySet();
	}
	
	public void mrSerialization(OutputStream outputStream, String encoding, String itemSeparator, String mrColumnSeparator) throws IOException {
		if (this.schemaCount == 0) {
			return;
		}
		List<String> lineList = new ArrayList<String>();
		lineList.add(StringUtils.join(this.allKeySchema.keys(), itemSeparator));
		lineList.add(StringUtils.join(this.maxSchema.keys(), itemSeparator));
		lineList.add(StringUtils.join(this.minSchema.keys(), itemSeparator));
		lineList.add(StringUtils.join(this.mostSchema.keys(), itemSeparator) + mrColumnSeparator + this.mostSchemaCount);
		lineList.add(String.valueOf(this.schemaCount));
		if (this.allSchemaCountFlag) {
			for (Entry<Schema, Long> entry : this.schemaCountMap.entrySet()) {
				lineList.add(StringUtils.join(entry.getKey().keys(), itemSeparator) + mrColumnSeparator + entry.getValue());
			}
		}
		IOUtils.writeLines(lineList, IOUtils.LINE_SEPARATOR, outputStream, encoding);
		outputStream.flush();
	}
	
	public static SchemaManager Parse(InputStream inputStream, String encoding, boolean allFlag, String itemSeparator, String mrColumnSeparator) throws Exception {
		SchemaManager schemaManager = new SchemaManager(allFlag);
		schemaManager.minSchema = new Schema();
		List<String> lineList = IOUtils.readLines(inputStream, encoding);
		if (lineList == null || lineList.size() < 5) {
			return schemaManager;
		}
		String[] items = lineList.get(0).split(mrColumnSeparator)[0].split(itemSeparator);
		for (String item : items) {
			schemaManager.allKeySchema.addKey(item);
		}
		items = lineList.get(1).split(mrColumnSeparator)[0].split(itemSeparator);
		for (String item : items) {
			schemaManager.maxSchema.addKey(item);
		}
		items = lineList.get(2).split(mrColumnSeparator)[0].split(itemSeparator);
		for (String item : items) {
			schemaManager.minSchema.addKey(item);
		}
		items = lineList.get(3).split(mrColumnSeparator);
		String[] itemsTemp = items[0].split(itemSeparator);
		for (String item : itemsTemp) {
			schemaManager.mostSchema.addKey(item);
		}
		schemaManager.mostSchemaCount = Long.parseLong(items[1]);
		schemaManager.schemaCount = Long.parseLong(lineList.get(4).split(mrColumnSeparator)[0]);
		Schema schema = null;
		long count = 0;
		if (allFlag) {
			for (int i = 5; i < lineList.size(); i++) {
				items = lineList.get(i).split(mrColumnSeparator);
				itemsTemp = items[0].split(itemSeparator);
				schema = new Schema();
				for (String item : itemsTemp) {
					schema.addKey(item);
				}
				count = Long.parseLong(items[1]);
				schemaManager.schemaCountMap.put(schema, count);
			}
		}
		return schemaManager;
	}
	
	public Schema getAllKeySchema() {
		return allKeySchema;
	}

	public void setAllKeySchema(Schema allKeySchema) {
		this.allKeySchema = allKeySchema;
	}

	public Schema getMaxSchema() {
		return maxSchema;
	}

	public void setMaxSchema(Schema maxSchema) {
		this.maxSchema = maxSchema;
	}

	public Schema getMinSchema() {
		if (minSchema == null) {
			this.minSchema = new Schema();
		}
		return minSchema;
	}

	public void setMinSchema(Schema minSchema) {
		this.minSchema = minSchema;
	}

	public Schema getMostSchema() {
		return mostSchema;
	}

	public void setMostSchema(Schema mostSchema) {
		this.mostSchema = mostSchema;
	}

	public long getMostSchemaCount() {
		return mostSchemaCount;
	}

	public void setMostSchemaCount(long mostSchemaCount) {
		this.mostSchemaCount = mostSchemaCount;
	}

	public long getSchemaCount() {
		return schemaCount;
	}

	public void setSchemaCount(long schemaCount) {
		this.schemaCount = schemaCount;
	}

	public boolean isAllSchemaCountFlag() {
		return allSchemaCountFlag;
	}

	public void setAllSchemaCountFlag(boolean allSchemaCountFlag) {
		this.allSchemaCountFlag = allSchemaCountFlag;
	}

}
