package org.pplm.hadoop.gadgets.hdfs.traversal.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author OracleGao
 *
 */
public class ReflectUtils {
	
	public static List<Field> ReflectDeclaredFields(Class<?> clazz) {
		List<Field> fieldList = new ArrayList<Field>();
		Field[] fields = null;
		Class<?> clazzTemp = clazz;
		do {
			fields = clazzTemp.getDeclaredFields();
			if (fields != null) {
				for (Field field : fields) {
					fieldList.add(field);
				}
			}
		} while((clazzTemp = clazzTemp.getSuperclass()) != null);
		return fieldList;
	} 
	
	public static Field GetField(List<Field> fieldList, String name) {
		for (Field field : fieldList) {
			if (field.getName().equals(name)) {
				return field;
			}
		}
		return null;
	}
}
