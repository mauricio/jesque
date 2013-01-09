package com.officedrop.jesque.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class DaemonThreadFactory implements ThreadFactory {

	private static final ThreadFactory instance = new DaemonThreadFactory();
	
	private ThreadFactory factory = Executors.defaultThreadFactory();
	
	private DaemonThreadFactory() {}
	
	public static ThreadFactory getInstance() {
		return instance;
	}
	
	@Override
	public Thread newThread(Runnable runnable) {
		
		Thread t = this.factory.newThread(runnable);
		t.setDaemon(true);
		
		return t;
	}

}
