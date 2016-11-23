package org.pplm.hadoop.gadgets.hdfs.traversal.utils;

import java.util.ArrayList;
import java.util.List;

public abstract class Listener<T> {

	protected List<T> listeners = new ArrayList<T>();
	private ClassLoader classLoader = Listener.class.getClassLoader();

	public Listener() {
		super();
		
	}

	protected abstract void initListener(T t);
	
	public void addListener(Class<? extends T> clazz) throws InstantiationException, IllegalAccessException {
		addListener(clazz.newInstance());
	}

	public void addListener(T t) {
		initListener(t);
		this.listeners.add(t);
	}
	
	@SuppressWarnings("unchecked")
	public void addListener(String className) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		addListener((Class<T>) Class.forName(className, true, classLoader));
	}
	
	public List<T> getListeners() {
		return listeners;
	}

	public void setListeners(List<T> listeners) {
		this.listeners = listeners;
	}
	
	public int size() {
		return this.listeners.size();
	}
	
}
