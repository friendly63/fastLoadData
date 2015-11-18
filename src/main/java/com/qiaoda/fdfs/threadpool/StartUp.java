package com.qiaoda.fdfs.threadpool;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import com.qiaoda.fdfs.fastLoadData.SpringContextUtils;

public class StartUp {
	
	private static final Logger logger = Logger.getLogger(StartUp.class);

	int coresize;
	int maxsize;
	int queuesize;
	String waitkey;
	long sleep;
	public void init(){
		InputStream in = this.getClass().getClassLoader().getResourceAsStream("fdfs-client.properties");
		Properties p = new Properties();
		try {
			p.load(in);
			
			coresize = Integer.parseInt(p.getProperty("fdfs.thread.minsize"));
			maxsize  = Integer.parseInt(p.getProperty("fdfs.thread.maxsize"));
			queuesize = Integer.parseInt(p.getProperty("fdfs.queue.size"));
			waitkey = p.getProperty("fdfs.wait_queue");
			
//			
//			JedisPoolConfig redisPoolConf = new JedisPoolConfig();
//	        redisPoolConf.setMaxTotal(1024);
//	        redisPoolConf.setMaxIdle(200);
//	        redisPoolConf.setMaxWaitMillis(1000);
//	        redisPoolConf.setTestOnBorrow(true);
//	        jp = new JedisPool(redisPoolConf, "10.18.99.147", 7002,Protocol.DEFAULT_DATABASE);

		} catch (IOException e) {
			logger.info("初始化失败"+e);
			e.printStackTrace();
		}
		
	}

	public void run() {
		if(true){
			init();
			logger.info("coresize:"+coresize+",maxsize:"+maxsize+",queuesize:"+queuesize);
			LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(queuesize);
			ThreadPoolExecutor executor = new ThreadPoolExecutor(
					coresize, 
					maxsize, 
					5000L, 
					TimeUnit.MICROSECONDS, 
					queue, 
					new RejectedExecutionReputHandler());
			while (true) {
				executor.submit(new FastDFSUploadTask());
			}
		}
			
		}
			
	public static void main(String[] args) {
		ApplicationContext context = new ClassPathXmlApplicationContext("classpath*:spring-config.xml");
        SpringContextUtils util = context.getBean(SpringContextUtils.class);
        util.setApplicationContext(context);
        StartUp stu = new StartUp();
        stu.run();
	}

}
