package com.qiaoda.fdfs.fastLoadData;

import java.io.File;

import net.mikesu.fastdfs.FastdfsClient;
import net.mikesu.fastdfs.FastdfsClientFactory;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	FastdfsClient fastdfsClient = FastdfsClientFactory.getFastdfsClient();
//		URL fileUrl = this.getClass().getResource("12311.jpg");
//		File file = new File(fileUrl.getPath());
		File file = new File("12311.jpg");
		String fileId;
		try {
			fileId = fastdfsClient.upload(file);
			String url = fastdfsClient.getUrl(fileId);
			System.out.println(url);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		log.info("fileId:"+fileId);
//		assertNotNull(fileId);
	
        
    }
}
