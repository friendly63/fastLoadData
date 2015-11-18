package com.qiaoda.fdfs.threadpool;

//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertTrue;
import java.io.File;
import java.net.URL;
import java.util.concurrent.Callable;

import net.mikesu.fastdfs.FastdfsClient;
import net.mikesu.fastdfs.FastdfsClientFactory;

import org.apache.log4j.Logger;




/**
 * 测试并发上传 Fastdfs 的任务类
 * 
 * FastDFS客户端使用开源 项目 https://github.com/baoming/FastdfsClient
 * 
 * 4并发60队列40000文件（50k），上传耗时180秒，约220/秒，达到性能要求
 * 
 * @author xianw
 *
 */
public class FastDFSUploadTask implements Callable{
	private static final Logger logger = Logger.getLogger(FastDFSUploadTask.class);
	
	public String call() throws Exception {

        try{
        	// 测试并发上传文件
        	uploadFileGenerator();
        	
        }catch(Exception e){
        	logger.error("upload fail", e);
        	
        }
        
//        jedis.close();
		return null;
	}

	private String uploadFileGenerator() throws Exception{

		FastdfsClient fastdfsClient = FastdfsClientFactory.getFastdfsClient();
//		URL fileUrl = this.getClass().getResource("12311.jpg");
//		File file = new File(fileUrl.getPath());
		File file = new File("12311.jpg");
		String fileId = fastdfsClient.upload(file);
//		log.info("fileId:"+fileId);
//		assertNotNull(fileId);
		String url = fastdfsClient.getUrl(fileId);
		System.out.println(url);
//		assertNotNull(url);
//		log.info("url:"+url);
//		Map<String,String> meta = new HashMap<String, String>();
//		meta.put("fileName", file.getName());
//		boolean result = fastdfsClient.setMeta(fileId, meta);
//		assertTrue(result);
//		Map<String,String> meta2 = fastdfsClient.getMeta(fileId);
//		assertNotNull(meta2);
//		log.info(meta2.get("fileName"));
//		result = fastdfsClient.delete(fileId);
//		assertTrue(result);
//		//fastdfsClient.close();
		
		return null;
	}	
	
	

}
