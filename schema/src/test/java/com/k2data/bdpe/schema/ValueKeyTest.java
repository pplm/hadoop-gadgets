package com.k2data.bdpe.schema;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.pplm.hadoop.gadgets.schema.key.KeyParseException;
import org.pplm.hadoop.gadgets.schema.key.KeysManager;
import org.pplm.hadoop.gadgets.schema.key.ValueKey;

public class ValueKeyTest {
	@Test
	public void keyManagerTest() throws IOException, KeyParseException {
		String file = Thread.currentThread().getContextClassLoader().getResource("key_test.csv").getPath();
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(file);
			List<String> list = IOUtils.readLines(inputStream);
			KeysManager keysManager = new KeysManager();
			keysManager.initKey(list);
			assertEquals(keysManager.validate("ST_BATTERYVOL", "-1"), ValueKey.VALID);
			assertEquals(keysManager.validate("ST_BATTERYVOL", "-10"), ValueKey.INVALD_LESS_THAN_MIN);
			assertEquals(keysManager.validate("ST_BATTERYVOL", "-10.0"), ValueKey.INVALD_DATA_TYPE);
			assertEquals(keysManager.validate("RE_EN_PID", "10002"), ValueKey.VALID);
			assertEquals(keysManager.validate("RE_EN_PID", "1000211"), ValueKey.INVALD_GREAT_THAN_MAX);
			assertEquals(keysManager.validate("RE_EN_PID", "-2"), ValueKey.INVALD_SIGNED | ValueKey.INVALD_LESS_THAN_MIN);
			assertEquals(keysManager.validate("ST_ALTITUDE", "false"), ValueKey.VALID);
			assertEquals(keysManager.validate("ST_ALTITUDE", "1false1"), ValueKey.INVALD_DATA_TYPE);
			assertEquals(keysManager.validate("ST_ALTITUDE", "0"), ValueKey.VALID);
			assertEquals(keysManager.validate("ST_ALTITUDE", "1"), ValueKey.VALID);
			assertEquals(keysManager.validate("ST_ALTITUDE", "01"), ValueKey.INVALD_DATA_TYPE);
			assertEquals(keysManager.validate("ST_ALTITUDE", "true"), ValueKey.VALID);
			assertEquals(keysManager.validate("ST_ENGV", "0.000"), ValueKey.INVALD_LESS_THAN_MIN | ValueKey.INVALD_PRECISION);
			assertEquals(keysManager.validate("ST_ENGV", "10.00"), ValueKey.VALID);
			assertEquals(keysManager.validate("ST_BERRORCODE", "-210.00"), ValueKey.INVALD_LESS_THAN_MIN | ValueKey.INVALD_SIGNED | ValueKey.INVALD_PRECISION);
			assertEquals(keysManager.validate("ST_FLOATRESERV30", "10.00"), ValueKey.VALID);
			System.out.println();
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
		
	}
}
