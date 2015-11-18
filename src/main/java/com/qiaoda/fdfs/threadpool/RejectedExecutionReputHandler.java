package com.qiaoda.fdfs.threadpool;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.log4j.Logger;

public class RejectedExecutionReputHandler implements RejectedExecutionHandler{
	private static final Logger logger = Logger.getLogger(RejectedExecutionReputHandler.class);
	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
		if (!executor.isShutdown()) {
			try {
				logger.info("call thread pool rejected!");
				Thread.sleep(200);
				executor.getQueue().put(r);
			} catch (InterruptedException e) {
				logger.info("error", e);
			}
		}
	}
	
	
}
