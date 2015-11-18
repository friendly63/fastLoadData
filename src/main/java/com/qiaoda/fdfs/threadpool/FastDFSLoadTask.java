package com.qiaoda.fdfs.threadpool;

//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import net.mikesu.fastdfs.FastdfsClient;
import net.mikesu.fastdfs.FastdfsClientFactory;
import net.mikesu.fastdfs.FastdfsClientImpl;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.csource.common.MyException;
import org.springframework.beans.factory.annotation.Autowired;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qiaoda.fdfs.fastLoadData.RedisPoolBean;
import com.qiaoda.fdfs.fastLoadData.SpringContextUtils;




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
public class FastDFSLoadTask implements Callable{
	
	private static final Logger logger = Logger.getLogger(FastDFSLoadTask.class);
	private final static HashMap<String,Object> fdfsMap = new HashMap<String,Object>();

	private final static Set<HostAndPort> jchaps = new HashSet<HostAndPort>();
//	private static MongoConnection con = null;

	@Autowired
	private JedisCluster jedisCluster;


	public String call() throws Exception {
		
        try{
        	init();
        	long strt = System.currentTimeMillis();
       	
        	long startime = System.currentTimeMillis();
        	uploadFileGenerator();
        	logger.info("整体耗时："+(System.currentTimeMillis()-strt));
        }catch(Exception e){
        	//上传失败网络异常等处理到这里
        	logger.error("upload file fail", e);
        	
        }
		return null;
	}
	public void init(){
		try {
			if(fdfsMap.isEmpty()){
				logger.info("init fastdfs conf");
				InputStream in = this.getClass().getClassLoader().getResourceAsStream("fdfs-client.properties");
				Properties p = new Properties();
				p.load(in);

				
				fdfsMap.put("wait_queue",  p.getProperty("fdfs.wait_queue"));
				fdfsMap.put("new_files",  p.getProperty("fdfs.new_files"));
				fdfsMap.put("sleep", p.getProperty("fdfs.thread.sleep"));

				in.close();
				
				logger.info("jedispool init end");
				InputStream in1 = this.getClass().getClassLoader().getResourceAsStream("redis.properties");
				Properties pt = new Properties();
				pt.load(in1);
				
				jchaps.add(new HostAndPort(pt.getProperty("address1"),Integer.parseInt(pt.getProperty("port1"))));
				jchaps.add(new HostAndPort(pt.getProperty("address2"),Integer.parseInt(pt.getProperty("port2"))));
				jchaps.add(new HostAndPort(pt.getProperty("address3"),Integer.parseInt(pt.getProperty("port3"))));
				jchaps.add(new HostAndPort(pt.getProperty("address4"),Integer.parseInt(pt.getProperty("port4"))));
				

				in1.close();
				logger.info("wait_queue:"+fdfsMap.get("wait_queue").toString()+",new_files:"+fdfsMap.get("new_files").toString());
			}
			
		} catch (IOException e) {	
			e.printStackTrace();
			logger.error("init fail, com.qiaoda.fdfs.FDFSLoadData.initConf() "+e);
		}
	}
	private String getRedisString() {

		String redisStr=null;
		JedisCluster jc = (JedisCluster)SpringContextUtils.getBean("jedisCluster");
		try{
			redisStr = jc.lpop(fdfsMap.get("wait_queue").toString());
		}catch(Exception e){
			logger.error("lpop redis error: "+e);
//			jc.close();
		}
//		jc.close();
		return redisStr;
	}
	/**
	 * single record fdfsFilePath   upload file to fdfs server,return filePath in storage server path
	 * @param fileName 
	 * @param filePath 
	 * @return return fdfsFilePath
	 */
	public String uploadFile1(FastdfsClient fastdfsClient,String fileName, String filePath,File file){
		String fileId = null ;
		try {
		fileId = fastdfsClient.upload(file);
		Map<String,String> meta = new HashMap<String, String>();
		meta.put("fileName", file.getName());
		meta.put("filePath", filePath);
		boolean result = fastdfsClient.setMeta(fileId, meta);
		logger.info("上传结束fileId "+fileId);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("upload fail,com.qiaoda.fdfs.threadpool.FastDFSLoadTask.uploadFile1(): "+e);
		} catch (MyException e) {
			e.printStackTrace();
			logger.error("upload fail, com.qiaoda.fdfs.threadpool.FastDFSLoadTask.uploadFile1(): "+e);
		} catch (Exception e) {
			logger.error("upload fail, com.qiaoda.fdfs.threadpool.FastDFSLoadTask.uploadFile1(): "+e);
			e.printStackTrace();
		}
		return fileId;
	}
	//保存到fdfs存储
	public void saveToFdfs(String filePath,FastdfsClient fastdfsClient,String jsonString,int a){
		
		if(a==2){
			logger.info("处理方式是2："+a+",filePath is"+filePath);
		}
		File f = new File(filePath);
		if(f.isFile()){
			String filename =filePath.substring(filePath.lastIndexOf("/")+1);
			long starttime = System.currentTimeMillis();
			String fileId = "";
			try{
				fileId=uploadFile1(fastdfsClient,filename,filePath,f);
			}catch(Exception e){
				logger.info("上传失败,rpush 原队列, exception:"+e);
				rpush(fdfsMap.get("wait_queue").toString(),jsonString);
				
			}
			
			long endtime = System.currentTimeMillis();
			logger.info("上传到存储耗时："+(endtime-starttime)+"毫秒");
			if(fileId==null){
				rpush(fdfsMap.get("wait_queue").toString(),jsonString);
				logger.info("upload fail,com.qiaoda.fdfs.FDFSLoadData.saveToFDFSandMongo(),right push waitqueue filePath:"+filePath);
			}else{
				if(a==1){
					JSONObject json = JSON.parseObject(jsonString);
//					String filePath = json.getString("filename");
					String from_id = json.getString("from_id");
					if(from_id.equals("fen_zip")){//处理2015815之前的压缩包
						boolean del = f.delete();
						if(del){
							logger.info("upload fdfs success,del fen_zip file success");
						}
					}
					String group = fileId.substring(0,fileId.indexOf("M00/")-1);
					String file = fileId.substring(fileId.indexOf("M00/"));
					try {
						String fileinfo = fastdfsClient.getUrl(fileId);
						String user_id = json.getString("user_id");
						RedisPoolBean redisBean = (RedisPoolBean)SpringContextUtils.getBean("redisPoolBean");
						redisBean.setFilename(fileinfo);
						redisBean.setFrom_id(from_id);
						redisBean.setFn2(filePath);
						redisBean.setUser_id(user_id);
						String redisJson = JSON.toJSONString(redisBean);
						pushNewQueue(fdfsMap.get("new_files").toString(),redisJson);
						logger.info("上传到new_files耗时："+(System.currentTimeMillis()-endtime)+"毫秒");
						logger.info("upload success ,push new queue new_files,new json: "+redisJson);
					} catch (Exception e) {
						logger.info("生成url错误,rpush wait_queue");
						rpush(fdfsMap.get("wait_queue").toString(),jsonString);
					}
				
				}else{
					RedisPoolBean redisBean = (RedisPoolBean)SpringContextUtils.getBean("redisPoolBean");
					redisBean.setFilename(fileId);
					redisBean.setFrom_id("fen_zip");
					redisBean.setFn2(filePath);
					redisBean.setUser_id("123");
					String redisJson = JSON.toJSONString(redisBean);
					pushNewQueue(fdfsMap.get("new_files").toString(),redisJson);
					logger.info("upload success ,push new queue new_files,new json: "+redisJson);
				}

			
			}
		}else{
			
			logger.info("upload fail com.qiaoda.fdfs.FDFSLoadData.saveToFDFSandMongo() ,file not exits, filePath:"+filePath);
			rpush(fdfsMap.get("wait_queue").toString(),jsonString);
			logger.info("rpush waitqueue "+filePath);
		}
	}
	public void saveToFDFSandMongo(FastdfsClient fastdfsClient ,String jsonString){
		if(jsonString==null)
			return ;
		if(jsonString.contains("filename")){
			JSONObject json = JSON.parseObject(jsonString);
			String filePath = json.getString("filename");
			saveToFdfs(filePath,fastdfsClient,jsonString,1);
		}else{
			saveToFdfs(jsonString,fastdfsClient,jsonString,2);
		}
		
		
		
	}
	/**
	 * upload success,create new jsonstring,left push new queue
	 * @param keys
	 * @param jsonObject
	 */
	public void pushNewQueue(String keys,String jsonObject){
		JedisCluster jc = (JedisCluster)SpringContextUtils.getBean("jedisCluster");
		jc.lpush(keys, jsonObject);
	}
	
	/**
	 * fail upload ,right push queue
	 * @param keys
	 * @param jsonObject
	 */
	public void rpush(String keys ,String jsonObject){
		JedisCluster jc = (JedisCluster)SpringContextUtils.getBean("jedisCluster");
		jc.rpush(keys, jsonObject);
	}
	/**
	 * 
	 * @param fdfsPath fastdfs filepath
	 * @param qfilePath queue filepath
	 * @param fmId from_id fen
	 * @param userId 
	 */
	public void saveToMongo(String fdfsPath,String qfilePath,String fmId,String userId){
		Document doc = new Document();
		doc.append("_id", fdfsPath).append("fn", qfilePath).append("fm_id", fmId).append("hash", "").append("tp", System.currentTimeMillis()).append("user_id", userId).append("stat", "");
//		con.insertMg(doc);
		logger.info("save mongodb success filepath:"+qfilePath);
	}

	private String uploadFileGenerator() throws Exception{

		FastdfsClient fastdfsClient = FastdfsClientFactory.getFastdfsClient();
		
		long starttime = System.currentTimeMillis();
	    String jedis = getRedisString();
	    logger.info("jedis :"+jedis);
	    logger.info("获取redis.wait_queue耗时："+(System.currentTimeMillis()-starttime)+"毫秒");
	    if(jedis==null){

	    	logger.info("redis is empty,sleep 10 sec");
	    	Thread.currentThread().sleep(Long.parseLong(fdfsMap.get("sleep").toString()) * 10 *1000L);

	    	logger.info("redis is empty,sleep 1 sec");
	    	Thread.currentThread().sleep(Long.parseLong(fdfsMap.get("sleep").toString()) * 10 *1000L);

	    }else{
		    saveToFDFSandMongo(fastdfsClient,jedis);
	    }
	    
		return null;
	}	
	
	public void close(){
	    
//	    if(con != null){
//	    	con.close();
//	    }
	}

}
